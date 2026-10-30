"""Repricer: price listings from ManaPool's market data, flag risers, and record price history.

Base price for each listing (settings in config/repricer.toml):
    max(lowest NM listing, market price) * (1 + adjustment_pct / 100), rounded to the cent,
    never below the floor. Foil listings use the foil fields of ManaPool's price feed.
    If the lowest NM listing equals our own price (it may be ours), the price is held or
    raised to the market anchor, never undercut, so repeated runs cannot ratchet it down.

A change is applied automatically unless it exceeds BOTH review limits (percent and dollars);
those go to a review list. Risers (lowest listing far above market, supply draining, or market
climbing within the window) use the same formula and are flagged in the report.

Commands:
    python -m station.repricer snapshot            # nightly: record price history, never changes prices
    python -m station.repricer sync [--apply]      # compare live ManaPool prices with the database
    python -m station.repricer run [--preview]     # reprice: drift check, plan, review, push, verify
    python -m station.repricer pins | pin ... | unpin ...

The repricer refuses to push while live ManaPool prices differ from the database
(run `sync --apply` first). Pushes send price only (quantity null).
"""

import argparse
import csv
import datetime
import os
import sys
import tomllib
from decimal import ROUND_HALF_UP, Decimal
from typing import Any, Callable, Dict, List, Optional, Tuple

from psycopg2 import extras
from tabulate import tabulate

from station import db
from station import manapool_client
from station import CONFIG_DIR

CONFIG_PATH = os.path.join(CONFIG_DIR, "repricer.toml")


def load_config(path: str = CONFIG_PATH) -> Dict[str, Any]:
    """Reads config/repricer.toml."""
    with open(path, "rb") as f:
        return tomllib.load(f)


def _cents(dollars: Any) -> Optional[int]:
    return None if dollars is None else int((Decimal(str(dollars)) * 100).quantize(Decimal("1"), ROUND_HALF_UP))


def _dollars(cents: Optional[int]) -> Optional[Decimal]:
    return None if cents is None else (Decimal(cents) / 100).quantize(Decimal("0.01"))


def _fmt(cents: Optional[int]) -> str:
    return "-" if cents is None else f"${cents / 100:.2f}"


# --------------------------------------------------------------------------- feed


def load_feed(config: Dict[str, Any]) -> Tuple[datetime.datetime, Dict[str, Dict[str, Any]]]:
    """Downloads the ManaPool price feed and checks its age.

    Returns:
        Tuple[datetime.datetime, Dict[str, Dict[str, Any]]]: (feed as_of in UTC, rows by scryfall_id).

    Raises:
        RuntimeError: If the feed has no timestamp or is older than [feed] max_age_hours.
    """
    as_of_str, rows = manapool_client.fetch_price_feed()
    if not as_of_str:
        raise RuntimeError("ManaPool price feed has no as_of timestamp; refusing to use it.")
    as_of = datetime.datetime.fromisoformat(as_of_str.replace("Z", "+00:00"))
    age = datetime.datetime.now(datetime.timezone.utc) - as_of
    max_age = datetime.timedelta(hours=config["feed"]["max_age_hours"])
    if age > max_age:
        raise RuntimeError(f"ManaPool price feed is {age} old (limit {max_age}); refusing to use it.")
    return as_of, {r["scryfall_id"]: r for r in rows if r.get("scryfall_id")}


def feed_prices(row: Optional[Dict[str, Any]], is_foil: bool) -> Tuple[Optional[int], Optional[int]]:
    """(lowest NM listing cents, market cents) for one finish of a feed row."""
    if not row:
        return None, None
    if is_foil:
        return row.get("price_cents_nm_foil"), row.get("price_market_foil")
    return row.get("price_cents_nm"), row.get("price_market")


# --------------------------------------------------------------------------- pricing


def base_price_cents(low: Optional[int], market: Optional[int], config: Dict[str, Any]) -> Optional[int]:
    """max(low, market) adjusted by adjustment_pct, rounded half up to the cent, floored."""
    anchors = [p for p in (low, market) if p is not None]
    if not anchors:
        return None
    adjusted = Decimal(max(anchors)) * (1 + Decimal(str(config["pricing"]["adjustment_pct"])) / 100)
    cents = int(adjusted.quantize(Decimal("1"), ROUND_HALF_UP))
    return max(cents, _cents(config["pricing"]["floor"]))


def needs_review(current: Optional[int], target: int, config: Dict[str, Any]) -> bool:
    """True if the change exceeds BOTH the percent and the dollar review limit."""
    if current is None or current == 0:
        return False
    delta = abs(target - current)
    pct = delta / current * 100
    return pct > config["review"]["max_auto_change_pct"] and delta > _cents(config["review"]["max_auto_change_dollars"])


def riser_signals(
    low: Optional[int],
    market: Optional[int],
    available: Optional[int],
    history_start: Optional[Dict[str, Any]],
    is_foil: bool,
    config: Dict[str, Any],
) -> List[str]:
    """Riser signals that fire for one listing (see [risers] in repricer.toml).

    Args:
        low, market: Current lowest NM listing and market price (cents) for this finish.
        available: Current ManaPool-wide available quantity for the card.
        history_start: Oldest price_history row for the card within the window, or None.
        is_foil: Which finish's history columns to compare.
        config: Repricer settings.

    Returns:
        List[str]: Fired signal names ('LOW>MARKET', 'SUPPLY DROP', 'MARKET RISE').
    """
    r = config["risers"]
    signals: List[str] = []
    if low is not None and market:
        if Decimal(low) >= Decimal(market) * Decimal(str(r["low_vs_market_ratio"])):
            signals.append("LOW>MARKET")
    if history_start:
        then_qty = history_start.get("available_quantity")
        if then_qty and available is not None:
            if (then_qty - available) / then_qty * 100 >= r["supply_drop_pct"]:
                signals.append("SUPPLY DROP")
        then_market = _cents(history_start.get("market_price_foil" if is_foil else "market_price_normal"))
        if then_market and market is not None:
            rise = market - then_market
            if rise / then_market * 100 >= r["market_rise_pct"] and rise >= _cents(r["market_rise_dollars"]):
                signals.append("MARKET RISE")
    return signals


# --------------------------------------------------------------------------- database


def load_products() -> List[Dict[str, Any]]:
    """One row per ManaPool product in inventory: identity, finish, total stock and listed price."""
    query = """
        SELECT i.manapool_product_id AS product_id,
               MIN(i.scryfall_id) AS scryfall_id,
               BOOL_OR(i.back_scryfall_id IS NOT NULL) AS is_combo,
               BOOL_OR(i.is_foil) AS is_foil,
               SUM(i.quantity) AS quantity,
               MAX(i.manapool_listed_price) AS listed_price,
               MIN(sc.name || COALESCE(' // ' || b.name, '')) AS name,
               MIN(sc.set_code) AS set_code,
               MIN(sc.collector_number || COALESCE('/' || b.collector_number, '')) AS collector_number
        FROM inventory i
        JOIN scryfall_cards sc ON i.scryfall_id = sc.scryfall_id
        LEFT JOIN scryfall_cards b ON i.back_scryfall_id = b.scryfall_id
        WHERE i.manapool_product_id IS NOT NULL AND i.manapool_product_id <> ''
        GROUP BY i.manapool_product_id;
    """
    with db.get_db_cursor(commit=False) as cur:
        cur.execute(query)
        rows = [dict(r) for r in cur.fetchall()]
    for r in rows:
        r["listed_cents"] = _cents(r["listed_price"])
    return rows


def load_pins() -> Dict[str, Optional[int]]:
    """product_id -> fixed price in cents (None = keep the listed price)."""
    with db.get_db_cursor(commit=False) as cur:
        cur.execute("SELECT manapool_product_id, fixed_price FROM price_pins;")
        return {r["manapool_product_id"]: _cents(r["fixed_price"]) for r in cur.fetchall()}


def load_history_starts(window_days: int) -> Dict[str, Dict[str, Any]]:
    """Oldest price_history row per card within the window (the comparison point for risers)."""
    query = """
        SELECT DISTINCT ON (scryfall_id) *
        FROM price_history
        WHERE recorded_date >= CURRENT_DATE - %s AND recorded_date < CURRENT_DATE
        ORDER BY scryfall_id, recorded_date;
    """
    with db.get_db_cursor(commit=False) as cur:
        cur.execute(query, (window_days,))
        return {r["scryfall_id"]: dict(r) for r in cur.fetchall()}


def set_listed_prices(prices: Dict[str, int]) -> None:
    """Writes new listed prices (cents) to every inventory row of each product."""
    with db.get_db_cursor() as cur:
        extras.execute_batch(
            cur,
            "UPDATE inventory SET manapool_listed_price = %s WHERE manapool_product_id = %s;",
            [(_dollars(c), pid) for pid, c in prices.items()],
        )


# --------------------------------------------------------------------------- snapshot


def snapshot(config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Records today's price history for every card in inventory (never changes prices).

    One row per Scryfall card (combo tokens excluded), upserted per day, so re-running the
    same day replaces that day's row. Cards missing from the feed get no row.

    Returns:
        Dict[str, Any]: {'recorded': int, 'missing': int, 'feed_as_of': datetime}.
    """
    config = config or load_config()
    as_of, feed = load_feed(config)
    query = """
        SELECT i.scryfall_id,
               MAX(i.manapool_listed_price) FILTER (WHERE NOT i.is_foil) AS listed_normal,
               MAX(i.manapool_listed_price) FILTER (WHERE i.is_foil) AS listed_foil
        FROM inventory i
        WHERE i.back_scryfall_id IS NULL
        GROUP BY i.scryfall_id;
    """
    with db.get_db_cursor(commit=False) as cur:
        cur.execute(query)
        cards = [dict(r) for r in cur.fetchall()]

    rows = []
    for c in cards:
        f = feed.get(c["scryfall_id"])
        if not f:
            continue
        rows.append((
            c["scryfall_id"],
            _dollars(f.get("price_market")), _dollars(f.get("price_market_foil")),
            _dollars(f.get("price_cents_nm")), _dollars(f.get("price_cents_nm_foil")),
            f.get("available_quantity"), c["listed_normal"], c["listed_foil"], as_of.replace(tzinfo=None),
        ))
    with db.get_db_cursor() as cur:
        extras.execute_values(cur, """
            INSERT INTO price_history (scryfall_id, market_price_normal, market_price_foil, low_price_normal,
                                       low_price_foil, available_quantity, listed_price_normal, listed_price_foil,
                                       feed_as_of)
            VALUES %s
            ON CONFLICT (scryfall_id, recorded_date) DO UPDATE SET
                market_price_normal = EXCLUDED.market_price_normal,
                market_price_foil = EXCLUDED.market_price_foil,
                low_price_normal = EXCLUDED.low_price_normal,
                low_price_foil = EXCLUDED.low_price_foil,
                available_quantity = EXCLUDED.available_quantity,
                listed_price_normal = EXCLUDED.listed_price_normal,
                listed_price_foil = EXCLUDED.listed_price_foil,
                feed_as_of = EXCLUDED.feed_as_of;
        """, rows, page_size=1000)
    return {"recorded": len(rows), "missing": len(cards) - len(rows), "feed_as_of": as_of}


# --------------------------------------------------------------------------- drift


def check_drift() -> Dict[str, Any]:
    """Compares live ManaPool prices (and quantities) with the database.

    Returns:
        Dict[str, Any]: 'price_drift' (product_id, db_cents, live_cents, name...), 'not_listed'
            (in-stock products with no live listing), 'qty_drift' (informational).
    """
    live = {i["product_id"]: i for i in manapool_client.fetch_seller_inventory()}
    price_drift, not_listed, qty_drift = [], [], []
    for p in load_products():
        item = live.get(p["product_id"])
        if item is None:
            if p["quantity"] > 0:
                not_listed.append(p)
            continue
        if item.get("price_cents") is not None and item["price_cents"] != p["listed_cents"]:
            price_drift.append({**p, "live_cents": item["price_cents"]})
        if (item.get("quantity") or 0) != p["quantity"]:
            qty_drift.append({**p, "live_quantity": item.get("quantity") or 0})
    return {"price_drift": price_drift, "not_listed": not_listed, "qty_drift": qty_drift}


def apply_drift(price_drift: List[Dict[str, Any]]) -> int:
    """Copies live ManaPool prices into the database for drifted products."""
    set_listed_prices({d["product_id"]: d["live_cents"] for d in price_drift})
    return len(price_drift)


# --------------------------------------------------------------------------- reprice


def plan_reprice(
    products: List[Dict[str, Any]],
    feed: Dict[str, Dict[str, Any]],
    pins: Dict[str, Optional[int]],
    history_starts: Dict[str, Dict[str, Any]],
    config: Dict[str, Any],
    not_listed: Optional[set] = None,
) -> List[Dict[str, Any]]:
    """Decides a target price for every in-stock product.

    When the lowest NM listing equals our listed price it is treated as our own listing:
    the price is held (or raised to the market anchor) instead of undercut. Products in
    `not_listed` (no live ManaPool listing) are skipped.

    Each returned item has 'status': 'auto', 'review', 'unchanged', 'pinned' or 'skipped',
    plus 'target_cents', 'low', 'market', 'signals' and 'reason' where relevant.
    """
    not_listed = not_listed or set()
    plan: List[Dict[str, Any]] = []
    for p in products:
        if p["quantity"] <= 0:
            continue
        item = {**p, "target_cents": None, "low": None, "market": None, "signals": [], "reason": ""}
        if p["product_id"] in not_listed:
            plan.append({**item, "status": "skipped", "reason": "not listed on ManaPool"})
            continue
        if p["product_id"] in pins:
            fixed = pins[p["product_id"]]
            if fixed is None or fixed == p["listed_cents"]:
                plan.append({**item, "status": "pinned", "reason": "pinned"})
            else:
                plan.append({**item, "status": "auto", "target_cents": fixed, "reason": "pinned fixed price"})
            continue
        if p["is_combo"]:
            plan.append({**item, "status": "skipped", "reason": "double-sided combo: no market data"})
            continue
        row = feed.get(p["scryfall_id"])
        low, market = feed_prices(row, p["is_foil"])
        item.update({"low": low, "market": market})
        if low is not None and low == p["listed_cents"]:
            # The lowest NM listing may be our own: undercutting it would ratchet our price down
            # every run. Hold the price unless the market anchor alone is higher.
            target = max(p["listed_cents"], base_price_cents(None, market, config) or 0)
            item["reason"] = "own listing is the lowest"
        else:
            target = base_price_cents(low, market, config)
        if target is None:
            plan.append({**item, "status": "skipped", "reason": "no market data"})
            continue
        item["target_cents"] = target
        item["signals"] = riser_signals(
            low, market, row.get("available_quantity") if row else None,
            history_starts.get(p["scryfall_id"]), p["is_foil"], config,
        )
        if target == p["listed_cents"]:
            status = "unchanged"
        elif needs_review(p["listed_cents"], target, config):
            status = "review"
        else:
            status = "auto"
        plan.append({**item, "status": status})
    return plan


def write_report(plan: List[Dict[str, Any]], decisions: Dict[str, Optional[int]], header: List[str],
                 output_dir: str = "exports") -> Tuple[str, str]:
    """Writes the run report (.txt) and a full CSV of every planned price."""
    os.makedirs(output_dir, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    txt_path = os.path.join(output_dir, f"reprice_{stamp}.txt")
    csv_path = os.path.join(output_dir, f"reprice_{stamp}.csv")

    def table(items: List[Dict[str, Any]], extra: Optional[Callable[[Dict[str, Any]], str]] = None) -> str:
        if not items:
            return "  (none)\n"
        return tabulate([[
            i["name"][:34], i["set_code"], i["collector_number"], "FOIL" if i["is_foil"] else "NF", i["quantity"],
            _fmt(i["listed_cents"]), _fmt(i["target_cents"]), _fmt(i["low"]), _fmt(i["market"]),
            ", ".join(i["signals"]), extra(i) if extra else "",
        ] for i in items], headers=["Card", "Set", "Num", "Fin", "Qty", "Now", "New", "Low", "Market", "Riser", "Note"],
            tablefmt="simple") + "\n"

    def delta(i: Dict[str, Any]) -> int:
        return (i["target_cents"] or 0) - (i["listed_cents"] or 0)

    risers = [i for i in plan if i["signals"]]
    reviews = [i for i in plan if i["status"] == "review"]
    autos = sorted([i for i in plan if i["status"] == "auto"], key=lambda i: -abs(delta(i)))
    skipped = [i for i in plan if i["status"] == "skipped"]

    def decision_note(i: Dict[str, Any]) -> str:
        if i["product_id"] not in decisions:
            return "not reviewed"
        d = decisions[i["product_id"]]
        return "rejected" if d is None else f"accepted {_fmt(d)}"

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("=" * 100 + "\n" + "REPRICE REPORT\n" + "=" * 100 + "\n")
        f.write("\n".join(header) + "\n" + "=" * 100 + "\n")
        f.write(f"\nRISERS ({len(risers)})\n" + table(risers, lambda i: i["status"]))
        f.write(f"\nREVIEW ({len(reviews)})\n" + table(reviews, decision_note))
        f.write(f"\nAUTOMATIC CHANGES ({len(autos)}, largest first)\n" + table(autos))
        f.write(f"\nSKIPPED ({len(skipped)})\n" + table(skipped, lambda i: i["reason"]))

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["status", "product_id", "name", "set", "number", "finish", "quantity", "now", "new",
                         "low", "market", "risers", "reason"])
        for i in plan:
            writer.writerow([i["status"], i["product_id"], i["name"], i["set_code"], i["collector_number"],
                             "FO" if i["is_foil"] else "NF", i["quantity"], _dollars(i["listed_cents"]),
                             _dollars(i["target_cents"]), _dollars(i["low"]), _dollars(i["market"]),
                             " ".join(i["signals"]), i["reason"]])
    return os.path.abspath(txt_path), os.path.abspath(csv_path)


def summarize(plan: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Counts and listed value before/after for the plan (value uses stock quantity)."""
    count = {s: sum(1 for i in plan if i["status"] == s) for s in ("auto", "review", "unchanged", "pinned", "skipped")}
    before = sum((i["listed_cents"] or 0) * i["quantity"] for i in plan)
    after_auto = sum(((i["target_cents"] if i["status"] == "auto" else i["listed_cents"]) or 0) * i["quantity"] for i in plan)
    return {**count, "risers": sum(1 for i in plan if i["signals"]), "value_before": before, "value_after_auto": after_auto}


def run_reprice(
    review: Optional[Callable[[List[Dict[str, Any]]], Dict[str, Optional[int]]]] = None,
    confirm: Optional[Callable[[List[str]], bool]] = None,
    preview: bool = False,
    config: Optional[Dict[str, Any]] = None,
    output_dir: str = "exports",
) -> Dict[str, Any]:
    """Runs a reprice: drift check, plan, review, confirm, price-only push, verify, update DB.

    Args:
        review: Called with the over-limit items; returns product_id -> accepted cents or None
            (rejected). Items left out are not changed. None = reject every review item.
        confirm: Called with summary lines before pushing; must return True to push.
            None = push without asking.
        preview: Plan and report only; never pushes (runs even if prices have drifted).
        config: Settings (default: repricer.toml).
        output_dir: Folder for the report.

    Returns:
        Dict[str, Any]: plan, summary, drift, decisions, report paths, push/verify results.
    """
    config = config or load_config()
    result: Dict[str, Any] = {"pushed": 0, "errors": [], "mismatches": [], "applied": {}, "blocked": None}

    drift = check_drift()
    result["drift"] = drift
    if drift["price_drift"] and not preview:
        result["blocked"] = (f"{len(drift['price_drift'])} live ManaPool prices differ from the database. "
                             "Run 'python -m station.repricer sync --apply' first.")
        return result

    as_of, feed = load_feed(config)
    plan = plan_reprice(load_products(), feed, load_pins(), load_history_starts(config["risers"]["window_days"]), config,
                        not_listed={p["product_id"] for p in drift["not_listed"]})
    summary = summarize(plan)
    result.update({"plan": plan, "summary": summary, "feed_as_of": as_of})

    review_items = [i for i in plan if i["status"] == "review"]
    decisions: Dict[str, Optional[int]] = {}
    if review_items and not preview:
        decisions = review(review_items) if review else {i["product_id"]: None for i in review_items}
    result["decisions"] = decisions

    changes = {i["product_id"]: i["target_cents"] for i in plan if i["status"] == "auto"}
    changes.update({pid: cents for pid, cents in decisions.items() if cents is not None})
    header = [
        f"Run:        {datetime.datetime.now():%Y-%m-%d %H:%M}{'   [PREVIEW - nothing pushed]' if preview else ''}",
        f"Feed as of: {as_of:%Y-%m-%d %H:%M} UTC",
        f"Rule:       max(lowest NM, market) {config['pricing']['adjustment_pct']:+}%, floor ${config['pricing']['floor']:.2f}; "
        f"review if > {config['review']['max_auto_change_pct']}% and > ${config['review']['max_auto_change_dollars']:.2f}",
        f"Products:   {summary['auto']} automatic, {summary['review']} for review, {summary['unchanged']} unchanged, "
        f"{summary['pinned']} pinned, {summary['skipped']} skipped; {summary['risers']} risers",
        f"Value:      {_fmt(summary['value_before'])} now -> {_fmt(summary['value_after_auto'])} after automatic changes",
        f"Drift:      {len(drift['price_drift'])} price, {len(drift['qty_drift'])} quantity, "
        f"{len(drift['not_listed'])} in stock but not listed",
        f"To push:    {len(changes)} price changes",
    ]
    result["txt_path"], result["csv_path"] = write_report(plan, decisions, header, output_dir)
    result["header"] = header
    if preview or not changes:
        return result
    if confirm is not None and not confirm(header):
        return result

    pushed, errors = manapool_client.push_prices(changes)
    result.update({"pushed": pushed, "errors": errors})
    live = {i["product_id"]: i.get("price_cents") for i in manapool_client.fetch_seller_inventory()}
    verified = {pid: c for pid, c in changes.items() if live.get(pid) == c}
    result["mismatches"] = [pid for pid in changes if pid not in verified]
    set_listed_prices(verified)
    result["applied"] = verified
    return result


# --------------------------------------------------------------------------- pins


def find_product_ids(set_code: str, number: str, foil: bool) -> List[str]:
    """ManaPool product ids in inventory for a set / collector number / finish."""
    with db.get_db_cursor(commit=False) as cur:
        cur.execute("""
            SELECT DISTINCT i.manapool_product_id
            FROM inventory i JOIN scryfall_cards sc ON i.scryfall_id = sc.scryfall_id
            WHERE sc.set_code = UPPER(%s) AND LOWER(sc.collector_number) = LOWER(%s) AND i.is_foil = %s
              AND i.manapool_product_id IS NOT NULL;
        """, (set_code, number, foil))
        return [r["manapool_product_id"] for r in cur.fetchall()]


def list_pins() -> List[Dict[str, Any]]:
    """All pins with the card they refer to."""
    with db.get_db_cursor(commit=False) as cur:
        cur.execute("""
            SELECT p.manapool_product_id, p.fixed_price, p.note, p.created_at,
                   MIN(sc.name) AS name, MIN(sc.set_code) AS set_code, MIN(sc.collector_number) AS collector_number,
                   BOOL_OR(i.is_foil) AS is_foil, MAX(i.manapool_listed_price) AS listed_price
            FROM price_pins p
            LEFT JOIN inventory i ON i.manapool_product_id = p.manapool_product_id
            LEFT JOIN scryfall_cards sc ON i.scryfall_id = sc.scryfall_id
            GROUP BY p.manapool_product_id, p.fixed_price, p.note, p.created_at
            ORDER BY MIN(sc.name);
        """)
        return [dict(r) for r in cur.fetchall()]


def add_pin(product_id: str, fixed_price: Optional[float] = None, note: Optional[str] = None) -> None:
    """Pins a product (optionally at a fixed price in dollars, never below the floor)."""
    floor = load_config()["pricing"]["floor"]
    if fixed_price is not None and fixed_price < floor:
        raise ValueError(f"Fixed price ${fixed_price:.2f} is below ManaPool's floor ${floor:.2f}.")
    with db.get_db_cursor() as cur:
        cur.execute("""
            INSERT INTO price_pins (manapool_product_id, fixed_price, note) VALUES (%s, %s, %s)
            ON CONFLICT (manapool_product_id) DO UPDATE SET fixed_price = EXCLUDED.fixed_price, note = EXCLUDED.note;
        """, (product_id, fixed_price, note))


def remove_pin(product_id: str) -> bool:
    """Removes a pin; returns True if one existed."""
    with db.get_db_cursor() as cur:
        cur.execute("DELETE FROM price_pins WHERE manapool_product_id = %s;", (product_id,))
        return cur.rowcount > 0


# --------------------------------------------------------------------------- CLI


def _cli_review(items: List[Dict[str, Any]]) -> Dict[str, Optional[int]]:
    print(f"\n{len(items)} changes exceed the review limit:")
    print(tabulate([[n, i["name"][:32], i["set_code"], i["collector_number"], "FOIL" if i["is_foil"] else "NF",
                     _fmt(i["listed_cents"]), _fmt(i["target_cents"]), _fmt(i["low"]), _fmt(i["market"]),
                     ", ".join(i["signals"])] for n, i in enumerate(items, 1)],
                   headers=["#", "Card", "Set", "Num", "Fin", "Now", "New", "Low", "Market", "Riser"],
                   tablefmt="simple"))
    choice = input("[a]ccept all, [r]eject all, or [o]ne by one? ").strip().lower()
    if choice == "a":
        return {i["product_id"]: i["target_cents"] for i in items}
    if choice != "o":
        return {i["product_id"]: None for i in items}
    decisions: Dict[str, Optional[int]] = {}
    for i in items:
        ans = input(f"{i['name']} ({i['set_code']} {i['collector_number']}) {_fmt(i['listed_cents'])} -> "
                    f"{_fmt(i['target_cents'])}: [a]ccept, [s]kip, or type a price: ").strip().lower()
        if ans == "a":
            decisions[i["product_id"]] = i["target_cents"]
        elif ans.replace(".", "", 1).isdigit():
            decisions[i["product_id"]] = max(_cents(ans), _cents(load_config()["pricing"]["floor"]))
        else:
            decisions[i["product_id"]] = None
    return decisions


def main() -> None:
    """CLI entry point for the repricer."""
    parser = argparse.ArgumentParser(description="ManaPool repricer and price history")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("snapshot", help="Record today's price history (never changes prices)")
    p_sync = sub.add_parser("sync", help="Compare live ManaPool prices with the database")
    p_sync.add_argument("--apply", action="store_true", help="Copy live prices into the database")
    p_run = sub.add_parser("run", help="Reprice (drift check, plan, review, push, verify)")
    p_run.add_argument("--preview", action="store_true", help="Plan and report only; push nothing")
    p_run.add_argument("--output-dir", default="exports")
    sub.add_parser("pins", help="List pinned products")
    p_pin = sub.add_parser("pin", help="Pin a product so the repricer leaves it alone (or holds a fixed price)")
    p_pin.add_argument("--product", help="ManaPool product id")
    p_pin.add_argument("--set", dest="set_code")
    p_pin.add_argument("--number")
    p_pin.add_argument("--foil", action="store_true")
    p_pin.add_argument("--price", type=float, help="Fixed price in dollars (omit to keep the listed price)")
    p_pin.add_argument("--note")
    p_unpin = sub.add_parser("unpin", help="Remove a pin")
    p_unpin.add_argument("--product", required=True)
    args = parser.parse_args()

    if args.command == "snapshot":
        res = snapshot()
        print(f"Recorded {res['recorded']:,} cards (feed as of {res['feed_as_of']:%Y-%m-%d %H:%M} UTC); "
              f"{res['missing']:,} cards had no ManaPool listings.")
    elif args.command == "sync":
        drift = check_drift()
        print(tabulate([[d["name"][:34], d["set_code"], d["collector_number"], "FOIL" if d["is_foil"] else "NF",
                         _fmt(d["listed_cents"]), _fmt(d["live_cents"])] for d in drift["price_drift"][:50]],
                       headers=["Card", "Set", "Num", "Fin", "Database", "ManaPool"], tablefmt="simple"))
        print(f"{len(drift['price_drift'])} price differences, {len(drift['qty_drift'])} quantity differences, "
              f"{len(drift['not_listed'])} in-stock products not listed on ManaPool.")
        if args.apply and drift["price_drift"]:
            print(f"Copied {apply_drift(drift['price_drift'])} live prices into the database.")
    elif args.command == "run":
        def confirm(lines: List[str]) -> bool:
            print("\n".join(lines))
            return input("Push these price changes to ManaPool? (y/n): ").strip().lower() == "y"

        res = run_reprice(review=_cli_review, confirm=confirm, preview=args.preview, output_dir=args.output_dir)
        if res["blocked"]:
            print(res["blocked"])
            return
        if args.preview:
            print("\n".join(res["header"]))
        print(f"Report: {res['txt_path']}\nCSV:    {res['csv_path']}")
        if res["pushed"] or res["errors"]:
            for e in res["errors"]:
                print(f"[ManaPool error] {e}", file=sys.stderr)
            print(f"Pushed {res['pushed']} prices; verified {len(res['applied'])}; {len(res['mismatches'])} mismatches.")
    elif args.command == "pins":
        pins = list_pins()
        print(tabulate([[p["name"] or "?", p["set_code"] or "", p["collector_number"] or "",
                         "FOIL" if p["is_foil"] else "NF", p["listed_price"], p["fixed_price"] or "(hold)",
                         p["note"] or "", p["manapool_product_id"]] for p in pins],
                       headers=["Card", "Set", "Num", "Fin", "Listed", "Fixed", "Note", "Product"], tablefmt="simple")
              if pins else "No pins.")
    elif args.command == "pin":
        ids = [args.product] if args.product else (
            find_product_ids(args.set_code, args.number, args.foil) if args.set_code and args.number else [])
        if len(ids) != 1:
            parser.error(f"pin needs --product or a --set/--number that matches exactly one product (found {len(ids)}).")
        add_pin(ids[0], args.price, args.note)
        print(f"Pinned {ids[0]}" + (f" at ${args.price:.2f}" if args.price is not None else " (holds listed price)"))
    elif args.command == "unpin":
        print("Unpinned." if remove_pin(args.product) else "No pin for that product.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
