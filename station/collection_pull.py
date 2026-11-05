"""Pull cards out of store inventory for a personal collection or deck, then sync ManaPool.

Search live inventory by name, or feed a decklist, and the tool picks which copies to
take (cheapest in-stock printing first, oldest section first), prints a pull sheet in
walk order, deducts the cards from PostgreSQL on confirmation, and takes the pulled copies
off the affected ManaPool listings (live quantity - pulled, read back). If the ManaPool API
is not configured or the push fails, it writes a CSV of absolute quantities to upload by hand.

Copies needed by ManaPool orders waiting to ship are held back unless --force is given
(orders already picked and deducted are not held: their copies are out of the database).

Pulled cards are not tracked after they leave inventory; to return a card, put it in the
to-be-sorted pile and ingest it again with the normal intake.

Usage:
    python -m station.collection_pull search "lightning bolt" [--set M10] [--foil]
    python -m station.collection_pull pull "Lightning Bolt" [--qty 4] [--set M10 --number 146] [--foil]
    python -m station.collection_pull pull --decklist mydeck.txt [--yes] [--force]
"""

import argparse
import csv
import datetime
import os
import re
import sys
from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

from tabulate import tabulate

from station import db
from station import import_live_inventory
from station import manapool_client
from station import pull_list_optimizer
from station import scryfall_mirror

# "4 Lightning Bolt", "4x Lightning Bolt (M10) 146", "1 Beast // Spider (TMID) 8-11 *F*"
DECK_LINE_RE = re.compile(
    r"^\s*(\d+)\s*x?\s+(.+?)(?:\s+\(([A-Za-z0-9]+)\)(?:\s+(\S+))?)?(?:\s+\*(F|FOIL)\*)?\s*$",
    re.IGNORECASE,
)
DECK_SECTION_HEADERS = {"deck", "mainboard", "sideboard", "commander", "companion", "maybeboard", "about"}


def fold(name: str) -> str:
    """Case- and accent-insensitive form of a card name (same rule as shelf sorting)."""
    return import_live_inventory.name_sort_key(name)


def load_stock() -> List[Dict[str, Any]]:
    """Loads every in-stock inventory row with its card identity and location.

    Returns:
        List[Dict[str, Any]]: Rows with inventory_id, scryfall_id, back_scryfall_id, name,
            set_code, collector_number, is_foil, quantity, price, product id and location.
    """
    query = """
        SELECT
            i.inventory_id, i.scryfall_id, i.back_scryfall_id,
            sc.name || COALESCE(' // ' || b.name, '') AS name,
            sc.set_code,
            sc.collector_number || COALESCE('/' || b.collector_number, '') AS collector_number,
            i.is_foil, i.condition, i.quantity, i.manapool_listed_price, i.manapool_product_id,
            i.box_label, i.section_label, i.index_num
        FROM inventory i
        JOIN scryfall_cards sc ON i.scryfall_id = sc.scryfall_id
        LEFT JOIN scryfall_cards b ON i.back_scryfall_id = b.scryfall_id
        WHERE i.quantity > 0;
    """
    with db.get_db_cursor(commit=False) as cur:
        cur.execute(query)
        return [dict(r) for r in cur.fetchall()]


def _fifo_key(row: Dict[str, Any]) -> Tuple[Any, ...]:
    m_sec = re.search(r"(\d+)", row["section_label"] or "")
    return (int(m_sec.group(1)) if m_sec else 999, row["index_num"])


def _pull_order_key(row: Dict[str, Any]) -> Tuple[Any, ...]:
    """Cheapest printing first (unpriced last), then oldest section, then index."""
    price = row["manapool_listed_price"]
    return (price is None, float(price) if price is not None else 0.0, *_fifo_key(row))


def search_stock(
    stock: List[Dict[str, Any]],
    text: str,
    set_code: Optional[str] = None,
    foil: Optional[bool] = None,
) -> List[Dict[str, Any]]:
    """Finds in-stock rows whose name contains `text` (case/accent-insensitive).

    Args:
        stock (List[Dict[str, Any]]): Output of `load_stock`.
        text (str): Search text.
        set_code (Optional[str]): Only this set.
        foil (Optional[bool]): Only foil (True) or non-foil (False) copies.

    Returns:
        List[Dict[str, Any]]: Matching rows sorted by name, then cheapest first.
    """
    needle = fold(text)
    hits = [
        r for r in stock
        if needle in fold(r["name"])
        and (set_code is None or r["set_code"] == set_code.upper())
        and (foil is None or r["is_foil"] == foil)
    ]
    return sorted(hits, key=lambda r: (fold(r["name"]), _pull_order_key(r)))


def name_matches(row_name: str, wanted: str) -> bool:
    """Exact (folded) name match; also accepts the front face of a double-faced card."""
    w = fold(wanted)
    folded = fold(row_name)
    return folded == w or folded.split(" // ")[0] == w


def parse_decklist(lines: Iterable[str]) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Parses decklist lines into pull requests.

    Accepts '4 Name', '4x Name', '4 Name (SET) number' and a trailing '*F*' for foil.
    Blank lines, comments ('#', '//') and section headers (Deck, Sideboard, ...) are skipped.

    Args:
        lines (Iterable[str]): Decklist text lines.

    Returns:
        Tuple[List[Dict[str, Any]], List[str]]: (requests, lines that could not be parsed).
    """
    requests_: List[Dict[str, Any]] = []
    unparsed: List[str] = []
    for raw in lines:
        line = raw.strip()
        if not line or line.startswith("#") or line.startswith("//"):
            continue
        if line.rstrip(":").lower() in DECK_SECTION_HEADERS:
            continue
        m = DECK_LINE_RE.match(line)
        if not m:
            unparsed.append(line)
            continue
        requests_.append({
            "qty": int(m.group(1)),
            "name": m.group(2).strip(),
            "set_code": m.group(3).upper() if m.group(3) else None,
            "number": m.group(4),
            "foil": True if m.group(5) else None,
        })
    return requests_, unparsed


def reserved_by_open_orders() -> Tuple[Optional[Dict[str, int]], str]:
    """Quantity per ManaPool product needed by orders waiting to ship and not yet picked.

    Orders in the `picked_orders` ledger are left out: their copies were already deducted
    from the database, so holding them again would hold copies twice.

    Returns:
        Tuple[Optional[Dict[str, int]], str]: (product_id -> reserved qty, or None if the
            orders could not be checked; human-readable status).
    """
    if not manapool_client.is_configured():
        return None, "ManaPool API not configured: open orders were NOT checked."
    items, errors = manapool_client.fetch_unfulfilled_orders()
    # Any error means some orders may be missing: treat the check as failed.
    if errors:
        return None, f"Could not check open orders ({len(errors)} errors): {errors[0]}"
    picked = pull_list_optimizer.picked_orders(it.get("order_id") for it in items)
    reserved: Dict[str, int] = defaultdict(int)
    for it in items:
        if it.get("manapool_product_id") and it.get("order_id") not in picked:
            reserved[str(it["manapool_product_id"])] += int(it.get("Qty", 1))
    status = f"Checked open orders: {sum(reserved.values())} copies across {len(reserved)} products reserved."
    if picked:
        status += f" {len(picked)} already-picked orders not held."
    return dict(reserved), status


def plan_pulls(
    stock: List[Dict[str, Any]],
    requests_: List[Dict[str, Any]],
    reserved: Optional[Dict[str, int]] = None,
    force: bool = False,
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Chooses which inventory copies to pull for each request.

    Candidates are rows matching the name (and set/number/foil when given), taken
    cheapest printing first, oldest section first. Copies of a ManaPool product that are
    needed by open orders are held back unless `force` is True.

    Args:
        stock (List[Dict[str, Any]]): Output of `load_stock`.
        requests_ (List[Dict[str, Any]]): Pull requests (qty, name, set_code, number, foil;
            optional 'ids' = (scryfall_id, back_scryfall_id) for an exact printing, or
            'inventory_id' for one exact inventory row).
        reserved (Optional[Dict[str, int]]): product_id -> quantity reserved by open orders.
        force (bool): Ignore open-order reservations.

    Returns:
        Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]: (picks with 'qty_to_pull',
            shortfalls with 'missing_qty', 'held_for_orders' and 'request').
    """
    exact_keys = [(r["set_code"], r["number"]) for r in requests_ if r.get("set_code") and r.get("number")]
    resolved = scryfall_mirror.resolve_manapool_cards(exact_keys) if exact_keys else {}

    # Free copies per product = total in stock - reserved by open orders.
    product_free: Dict[str, int] = defaultdict(int)
    for row in stock:
        product_free[row["manapool_product_id"] or f"inv-{row['inventory_id']}"] += row["quantity"]
    held_total: Dict[str, int] = {}
    if reserved and not force:
        for pid, qty in reserved.items():
            if pid in product_free:
                held = min(qty, product_free[pid])
                product_free[pid] -= held
                held_total[pid] = held

    row_left = {row["inventory_id"]: row["quantity"] for row in stock}
    picks: List[Dict[str, Any]] = []
    shortfalls: List[Dict[str, Any]] = []

    for req in requests_:
        if req.get("inventory_id"):
            candidates = [r for r in stock if r["inventory_id"] == req["inventory_id"]]
        elif req.get("ids"):
            candidates = [r for r in stock if (r["scryfall_id"], r["back_scryfall_id"]) == tuple(req["ids"])]
        elif req.get("set_code") and req.get("number"):
            res = resolved.get((req["set_code"].upper(), str(req["number"]).strip().lower()))
            want = (res["front"]["scryfall_id"], res["back"]["scryfall_id"] if res["back"] else None) if res else None
            candidates = [r for r in stock if want and (r["scryfall_id"], r["back_scryfall_id"]) == want]
        else:
            candidates = [
                r for r in stock
                if name_matches(r["name"], req["name"])
                and (req.get("set_code") is None or r["set_code"] == req["set_code"].upper())
            ]
        if req.get("foil") is not None:
            candidates = [r for r in candidates if r["is_foil"] == req["foil"]]
        candidates.sort(key=_pull_order_key)

        need = req["qty"]
        for row in candidates:
            if need == 0:
                break
            pid = row["manapool_product_id"] or f"inv-{row['inventory_id']}"
            take = min(need, row_left[row["inventory_id"]], product_free[pid])
            if take <= 0:
                continue
            row_left[row["inventory_id"]] -= take
            product_free[pid] -= take
            need -= take
            picks.append({**row, "qty_to_pull": take, "request": req})
        if need > 0:
            shortfalls.append({
                "request": req,
                "missing_qty": need,
                "held_for_orders": any(held_total.get(r["manapool_product_id"], 0) for r in candidates),
                "found_any": bool(candidates),
            })
    return picks, shortfalls


def _walk_key(p: Dict[str, Any]) -> Tuple[Any, ...]:
    """Box (natural) -> set -> section -> index; token chunks walk straight down their index."""
    m_box = re.search(r"(\d+)", p["box_label"] or "")
    sec = p["section_label"] or ""
    return (
        int(m_box.group(1)) if m_box else 999,
        p["box_label"],
        db.TOKENS_SET if sec.upper().startswith("TOKEN") else p["set_code"],
        *_fifo_key(p),
    )


def write_pull_sheet(
    picks: List[Dict[str, Any]],
    shortfalls: List[Dict[str, Any]],
    label: str,
    order_status: str,
    output_dir: str = "exports",
) -> str:
    """Writes a printable personal pull sheet in walk order.

    Returns:
        str: Absolute path to the sheet.
    """
    os.makedirs(output_dir, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    path = os.path.join(output_dir, f"personal_pull_{stamp}.txt")
    picks = sorted(picks, key=_walk_key)
    total = sum(p["qty_to_pull"] for p in picks)
    value = sum(p["qty_to_pull"] * float(p["manapool_listed_price"] or 0) for p in picks)

    with open(path, "w", encoding="utf-8") as f:
        f.write(f"""========================================================================================
                          PERSONAL COLLECTION PULL SHEET
========================================================================================
 Date:       {datetime.datetime.now().strftime('%Y-%m-%d %H:%M')}
 Source:     {label}
 Total:      {total} cards across {len(picks)} pick steps (listed value ${value:.2f})
 Orders:     {order_status}
 Path:       Box -> Set -> Section -> Index (token chunks: Box -> Section -> Index)
========================================================================================
""")
        f.write(tabulate(
            [[
                f"#{seq:02d}", p["box_label"], p["set_code"], p["section_label"], f"#{p['index_num']:03d}",
                p["name"][:32], p["collector_number"], "FOIL" if p["is_foil"] else "NF", p["qty_to_pull"],
                f"${float(p['manapool_listed_price']):.2f}" if p["manapool_listed_price"] is not None else "-",
                "[   ]",
            ] for seq, p in enumerate(picks, start=1)],
            headers=["Seq", "Box", "Set", "Section", "Index", "Card Name", "Num", "Finish", "Qty", "Price", "Done"],
            tablefmt="simple",
        ))
        if shortfalls:
            f.write("\n\n [!] NOT AVAILABLE (missing, short, or held for open orders):\n")
            f.write(tabulate(
                [[s["request"]["name"][:35], s["request"].get("set_code") or "", s["request"].get("number") or "",
                  s["missing_qty"], "held for open order" if s["held_for_orders"] else
                  ("not enough in stock" if s["found_any"] else "not in stock")] for s in shortfalls],
                headers=["Card", "Set", "Num", "Missing", "Why"],
                tablefmt="simple",
            ))
        f.write("\n========================================================================================\n")
    return os.path.abspath(path)


def sync_manapool(picks: List[Dict[str, Any]], output_dir: str = "exports") -> Dict[str, Any]:
    """Takes the pulled copies off ManaPool (live quantity - pulled, read back), or writes a CSV.

    Must run after the deduction is committed. The base is the LIVE quantity, read just before
    sending (`manapool_client.push_removed_stock`), not the database total: a copy that sold but
    has not been picked yet is already off the live listing but still in the database, so the
    database total would list it again. Quantity only (price null), so live prices are left
    alone; products with no live listing are not sent.

    If the API is not configured or the push fails, writes a CSV of absolute quantities to
    upload by hand: live - pulled when the live listing was read ('basis' 'live'), otherwise the
    database total ('basis' 'database'; lower any product with an unpicked order before uploading).

    Returns:
        Dict[str, Any]: 'mode' ('api' | 'csv' | 'none'), 'updated', 'errors', 'csv_path', 'basis',
            and from the push 'before' / 'sent' (product_id -> live quantity before / quantity
            sent), 'unlisted' and 'mismatches' (product_ids).
    """
    by_product: Dict[str, Dict[str, Any]] = {}
    pulled: Dict[str, int] = defaultdict(int)
    for p in picks:
        if p.get("manapool_product_id"):
            by_product.setdefault(p["manapool_product_id"], p)
            pulled[p["manapool_product_id"]] += p["qty_to_pull"]
    result: Dict[str, Any] = {"mode": "none", "updated": 0, "errors": [], "csv_path": None, "basis": None,
                              "before": {}, "sent": {}, "unlisted": [], "mismatches": []}
    if not by_product:
        result["errors"] = ["No ManaPool product IDs on pulled rows."]
        return result

    targets: Optional[Dict[str, int]] = None
    if manapool_client.is_configured():
        try:
            push = manapool_client.push_removed_stock(dict(pulled))
            result.update({k: push[k] for k in ("updated", "errors", "before", "sent", "unlisted", "mismatches")})
            if not push["errors"]:
                result.update({"mode": "api", "basis": "live"})
                return result
            targets = push["sent"]
        except Exception as e:  # keep the deduction; fall back to the CSV below
            result["errors"] = [f"Push failed: {e}"]
    if targets is None:
        targets = db.get_total_quantities_by_product_ids(list(by_product))
        result["basis"] = "database"
    else:
        result["basis"] = "live"

    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"manapool_sync_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["product_type", "product_id", "name", "set", "number", "finish", "condition", "price", "quantity"])
        for pid, qty in targets.items():
            p = by_product[pid]
            writer.writerow([
                "mtg_single", pid, p["name"], p["set_code"], p["collector_number"],
                "FO" if p["is_foil"] else "NF", p.get("condition") or "NM",
                p["manapool_listed_price"] if p["manapool_listed_price"] is not None else "", qty,
            ])
    result.update({"mode": "csv", "csv_path": os.path.abspath(path)})
    return result


def run_pull(
    requests_: List[Dict[str, Any]],
    label: str,
    confirm: Any = None,
    force: bool = False,
    output_dir: str = "exports",
) -> Dict[str, Any]:
    """Plans a pull, writes the sheet, and on confirmation deducts stock and syncs ManaPool.

    Args:
        requests_ (List[Dict[str, Any]]): Pull requests.
        label (str): Source description for the sheet header.
        confirm (Any): Callable(summary_lines) -> bool; None means confirmed.
        force (bool): Ignore open-order reservations (and pull even if orders could not be checked).
        output_dir (str): Folder for the sheet and any sync CSV.

    Returns:
        Dict[str, Any]: Summary including picks, shortfalls, sheet path, committed flag and sync result.
            'blocked' holds a message (and nothing else is done) if open orders could not be
            checked and `force` is False.
    """
    reserved, order_status = reserved_by_open_orders()
    if reserved is None and not force:
        return {"picks": [], "shortfalls": [], "sheet_path": None, "order_status": order_status,
                "committed": False, "sync": None,
                "blocked": f"{order_status} Nothing pulled; re-run with --force to pull without holding open-order copies."}
    if force:
        order_status += " (--force: reservations ignored)"
    picks, shortfalls = plan_pulls(load_stock(), requests_, reserved, force=force)
    sheet_path = write_pull_sheet(picks, shortfalls, label, order_status, output_dir)
    result: Dict[str, Any] = {
        "picks": picks, "shortfalls": shortfalls, "sheet_path": sheet_path,
        "order_status": order_status, "committed": False, "sync": None, "blocked": None,
    }
    if not picks:
        return result

    summary = [
        f"Pull {sum(p['qty_to_pull'] for p in picks)} cards ({len(picks)} pick steps) out of inventory.",
        f"{len(shortfalls)} requests not fully available." if shortfalls else "All requested cards available.",
        order_status,
        f"Sheet: {sheet_path}",
    ]
    if confirm is not None and not confirm(summary):
        return result

    pull_list_optimizer.commit_pick_deductions(picks)
    result["committed"] = True
    result["sync"] = sync_manapool(picks, output_dir)
    return result


def sync_lines(sync: Dict[str, Any]) -> List[str]:
    """Human-readable lines describing a `sync_manapool` result (CLI and TUI)."""
    lines: List[str] = []
    if sync["mode"] == "api":
        lines.append(f"ManaPool: took the pulled copies off {sync['updated']} listings (live quantity - pulled).")
        if sync["mismatches"]:
            lines.append(f"  Read back: {len(sync['mismatches'])} listings differ from what was sent: "
                         + ", ".join(sync["mismatches"][:5]))
    elif sync["mode"] == "csv":
        lines += [f"  [ManaPool] {e}" for e in sync["errors"]]
        lines.append(f"ManaPool update failed or not confirmed. Upload these absolute quantities by hand: {sync['csv_path']}")
        if sync["basis"] == "database":
            lines.append("  These are database totals: lower any product with an unpicked order by that "
                         "order's quantity first, or the sold copy is listed again.")
    else:
        lines.append(f"ManaPool: nothing to sync ({'; '.join(sync['errors'])}).")
    if sync["unlisted"]:
        lines.append(f"  {len(sync['unlisted'])} pulled products have no ManaPool listing (left alone).")
    return lines


def _print_result(res: Dict[str, Any]) -> None:
    if res["committed"]:
        print(f"Deducted {sum(p['qty_to_pull'] for p in res['picks'])} cards from inventory.")
        for line in sync_lines(res["sync"]):
            print(line)
    elif res["picks"]:
        print("Not confirmed: inventory unchanged.")
    else:
        print("Nothing available to pull.")


def main() -> None:
    """CLI entry point for collection_pull."""
    parser = argparse.ArgumentParser(description="Pull cards from store inventory into a personal collection")
    sub = parser.add_subparsers(dest="command", required=True)

    p_search = sub.add_parser("search", help="Search in-stock cards by name")
    p_search.add_argument("text", help="Part of the card name (case/accent-insensitive)")
    p_search.add_argument("--set", dest="set_code", help="Only this set code")
    p_search.add_argument("--foil", action="store_true", help="Only foil copies")

    p_pull = sub.add_parser("pull", help="Pull one card or a whole decklist")
    p_pull.add_argument("name", nargs="?", help="Card name (exact; front face is enough for double-faced cards)")
    p_pull.add_argument("--qty", type=int, default=1, help="Copies to pull (default 1)")
    p_pull.add_argument("--set", dest="set_code", help="Exact set code")
    p_pull.add_argument("--number", help="Exact collector number (with --set)")
    p_pull.add_argument("--foil", action="store_true", help="Only foil copies")
    p_pull.add_argument("--decklist", help="Decklist file ('4 Lightning Bolt', '1 Name (SET) 123', '*F*' for foil)")
    p_pull.add_argument("--yes", action="store_true", help="Do not ask for confirmation")
    p_pull.add_argument("--force", action="store_true", help="Also pull copies held for open ManaPool orders")
    p_pull.add_argument("--output-dir", default="exports")

    args = parser.parse_args()

    if args.command == "search":
        hits = search_stock(load_stock(), args.text, args.set_code, True if args.foil else None)
        if not hits:
            print("No in-stock cards match.")
            return
        print(tabulate(
            [[r["name"][:40], r["set_code"], r["collector_number"], "FOIL" if r["is_foil"] else "NF", r["quantity"],
              f"${float(r['manapool_listed_price']):.2f}" if r["manapool_listed_price"] is not None else "-",
              r["box_label"], r["section_label"], f"#{r['index_num']:03d}"] for r in hits],
            headers=["Card Name", "Set", "Num", "Finish", "Qty", "Price", "Box", "Section", "Index"],
            tablefmt="simple",
        ))
        return

    if args.decklist:
        with open(args.decklist, encoding="utf-8") as f:
            requests_, unparsed = parse_decklist(f)
        for line in unparsed:
            print(f"[skipped, could not parse] {line}", file=sys.stderr)
        label = os.path.basename(args.decklist)
    elif args.name:
        requests_ = [{
            "qty": args.qty, "name": args.name, "set_code": args.set_code,
            "number": args.number, "foil": True if args.foil else None,
        }]
        label = f"{args.qty} x {args.name}"
    else:
        parser.error("pull needs a card name or --decklist")

    def ask(summary: List[str]) -> bool:
        for line in summary:
            print(line)
        return input("Deduct these from inventory and sync ManaPool? (y/n): ").strip().lower() == "y"

    res = run_pull(requests_, label, confirm=None if args.yes else ask, force=args.force, output_dir=args.output_dir)
    if res["blocked"]:
        print(res["blocked"], file=sys.stderr)
        sys.exit(1)
    for s in res["shortfalls"]:
        why = "held for open order" if s["held_for_orders"] else ("not enough in stock" if s["found_any"] else "not in stock")
        print(f"[short] {s['request']['name']}: missing {s['missing_qty']} ({why})")
    print(f"Pull sheet: {res['sheet_path']}")
    _print_result(res)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
