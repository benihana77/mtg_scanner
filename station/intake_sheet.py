"""Intake sheets: build a ManaPool catalog CSV for one set, count it, and hand it to ingest.

A sheet replaces the manual "Export Catalog" download from the ManaPool website. It has the
same columns as that export, so ingest reads either one. Each sheet covers one set, NM only,
English, and one finish (non-foil by default, or foil only). It lists every card in the set
with quantity 0; fill in quantities on the TUI count screen or in a spreadsheet.

Where the data comes from:
    - The set's cards: the local Scryfall mirror. Tokens and emblems are left out (they are
      handled separately), and a set code made only of tokens is refused.
    - Products and prices: ManaPool's public /products/singles lookup. Mirror cards with no
      matching ManaPool product are left off the sheet and listed.
    - Starting price: the repricer rule, max(lowest NM, market) adjusted and floored
      (config/repricer.toml). Cards with no data get the floor. A product we already list
      keeps its pin, and is not undercut when the lowest listing is our own.

Each sheet has a hidden sidecar (.intake_<...>.json) recording when it was priced and the
pre-filled prices, so a refresh replaces only prices that were not edited by hand. After a
successful ingest the sheet and sidecar move to intake/ingested/.

Commands:
    python -m station.intake_sheet new SOI [--foil] [--fresh]   # build (or resume) a sheet
    python -m station.intake_sheet refresh <sheet.csv>          # re-pull prices, keep counts
    python -m station.intake_sheet list                         # sheets waiting to be ingested
"""

import argparse
import csv
import datetime
import json
import os
import re
import shutil
import sys
from typing import Any, Dict, List, Optional, Tuple

from tabulate import tabulate

from station import ROOT
from station import db
from station import manapool_client
from station import repricer
from station import scryfall_mirror
from station.import_live_inventory import _set_sort_key, collector_number_sort_key, name_sort_key

INTAKE_DIR = os.path.join(ROOT, "intake")

# Same columns, same order, as ManaPool's website catalog export.
SHEET_COLUMNS = [
    "product_type", "product_id", "name", "set", "number", "language", "finish", "condition",
    "market_low", "market_price", "market_price_foil", "price", "quantity",
]

# Resuming a sheet priced longer ago than this re-pulls its prices.
STALE_AFTER = datetime.timedelta(hours=24)

SHEET_NAME_RE = re.compile(r"^intake_([A-Z0-9]+)(_foil)?_(\d{4}-\d{2}-\d{2})(?:-(\d+))?\.csv$")


def _money(cents: Optional[int]) -> str:
    return "" if cents is None else f"{cents / 100:.2f}"


def price_cents(value: Any) -> int:
    """A sheet price cell in cents; blank or unreadable cells (hand edits like '$1') count as 0."""
    try:
        return repricer._cents(str(value).strip().lstrip("$") or 0) or 0
    except ArithmeticError:
        return 0


def ingested_dir() -> str:
    return os.path.join(INTAKE_DIR, "ingested")


# --------------------------------------------------------------------------- pricing


def sheet_price(
    product_id: str,
    low: Optional[int],
    market: Optional[int],
    listed: Optional[int],
    pins: Dict[str, Optional[int]],
    config: Dict[str, Any],
) -> Tuple[int, str]:
    """Starting price (cents) for one sheet row, and a note on how it was chosen.

    Args:
        product_id: ManaPool product ID.
        low: Lowest NM listing for this finish (cents), None if nobody lists it.
        market: Market price for this finish (cents), or None.
        listed: Our current listed price if we already have the product in stock, else None.
        pins: Repricer pins, product_id -> fixed cents (None = hold the listed price).
        config: Repricer settings.

    Returns:
        Tuple[int, str]: (price in cents, note: '', 'pinned', 'own listing is the lowest' or 'no data').
    """
    if product_id in pins and (pins[product_id] is not None or listed is not None):
        fixed = pins[product_id]
        return (fixed if fixed is not None else listed), "pinned"
    if low is not None and listed is not None and low == listed:
        # Same rule as the repricer: the lowest listing may be ours, so never undercut it.
        return max(listed, repricer.base_price_cents(None, market, config) or 0), "own listing is the lowest"
    target = repricer.base_price_cents(low, market, config)
    if target is None:
        return repricer._cents(config["pricing"]["floor"]), "no data"
    return target, ""


def variant_prices(card: Dict[str, Any], variant: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
    """(lowest listing, market) in cents for one ManaPool variant; a low_price of 0 means no listing."""
    low = variant.get("low_price") or None
    market = card.get("price_market_foil") if variant.get("finish_id") == "FO" else card.get("price_market")
    return low, market


def load_listed(product_ids: List[str]) -> Dict[str, int]:
    """Our listed price (cents) for each product we currently have in stock."""
    if not product_ids:
        return {}
    with db.get_db_cursor(commit=False) as cur:
        cur.execute("""
            SELECT manapool_product_id, MAX(manapool_listed_price) AS listed_price
            FROM inventory
            WHERE quantity > 0 AND manapool_product_id = ANY(%s)
            GROUP BY manapool_product_id;
        """, (list(product_ids),))
        return {r["manapool_product_id"]: repricer._cents(r["listed_price"])
                for r in cur.fetchall() if r["listed_price"] is not None}


# --------------------------------------------------------------------------- building


def build_rows(
    set_code: str,
    cards: List[Dict[str, Any]],
    products: List[Dict[str, Any]],
    foil: bool,
    config: Dict[str, Any],
    listed: Dict[str, int],
    pins: Dict[str, Optional[int]],
) -> Dict[str, Any]:
    """Turns mirror cards and ManaPool products into sheet rows (pure, no I/O).

    Args:
        set_code: Set code the sheet is for.
        cards: Mirror cards in the set (from scryfall_mirror.cards_in_set).
        products: ManaPool cards for those Scryfall IDs (from fetch_single_products).
        foil: True for a foil-only sheet, False for non-foil.
        config: Repricer settings.
        listed: Our listed price (cents) for products we already stock.
        pins: Repricer pins.

    Returns:
        Dict[str, Any]: 'rows' (sheet rows in shelf order), 'prefill' (product_id -> price
            string), 'missing' (mirror cards with no matching product, sorted by number),
            'tokens' (number of token/emblem cards left out) and 'no_data' (rows priced at
            the floor for lack of data).
    """
    finish = "FO" if foil else "NF"
    by_sid: Dict[str, Dict[str, Any]] = {}
    for p in products:
        for sid in p.get("scryfall_ids") or [p.get("scryfall_id")]:
            if sid:
                by_sid[sid] = p

    rows: List[Dict[str, str]] = []
    prefill: Dict[str, str] = {}
    missing: List[Dict[str, Any]] = []
    tokens = no_data = 0
    seen: set = set()
    for c in cards:
        if scryfall_mirror.is_token_layout(c["layout"]):
            tokens += 1
            continue
        product = by_sid.get(c["scryfall_id"])
        variant = next((v for v in (product or {}).get("variants", [])
                        if v.get("condition_id") == "NM" and v.get("finish_id") == finish
                        and v.get("language_id") == "EN"), None)
        if not variant:
            missing.append({"number": c["collector_number"], "name": c["name"],
                            "reason": "no ManaPool product" if not product else f"no NM {'foil' if foil else 'non-foil'}"})
            continue
        if variant["product_id"] in seen:
            continue
        seen.add(variant["product_id"])
        low, market = variant_prices(product, variant)
        price, note = sheet_price(variant["product_id"], low, market, listed.get(variant["product_id"]), pins, config)
        no_data += note == "no data"
        row = {
            "product_type": "mtg_single",
            "product_id": variant["product_id"],
            "name": product.get("name") or c["name"],
            "set": (product.get("set_code") or set_code).upper(),
            "number": product.get("number") or c["collector_number"],
            "language": "EN",
            "finish": finish,
            "condition": "NM",
            "market_low": _money(low),
            "market_price": _money(product.get("price_market")),
            "market_price_foil": _money(product.get("price_market_foil")),
            "price": _money(price),
            "quantity": "0",
        }
        rows.append(row)
        prefill[row["product_id"]] = row["price"]

    rows.sort(key=_set_sort_key)
    missing.sort(key=lambda m: collector_number_sort_key(m["number"]))
    return {"rows": rows, "prefill": prefill, "missing": missing, "tokens": tokens, "no_data": no_data}


def new_sheet_path(set_code: str, foil: bool, today: Optional[datetime.date] = None) -> str:
    """intake/intake_<SET>[_foil]_<date>.csv, with -2, -3... if that name is taken."""
    today = today or datetime.date.today()
    stem = f"intake_{set_code.upper()}{'_foil' if foil else ''}_{today.isoformat()}"
    path = os.path.join(INTAKE_DIR, f"{stem}.csv")
    n = 2
    while os.path.exists(path) or os.path.exists(os.path.join(ingested_dir(), os.path.basename(path))):
        path = os.path.join(INTAKE_DIR, f"{stem}-{n}.csv")
        n += 1
    return path


def create_sheet(set_code: str, foil: bool = False, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Builds a new intake sheet for one set and writes it to intake/.

    Returns:
        Dict[str, Any]: 'status' is 'created', 'not_in_mirror', 'token_set' or 'no_products'
            with a 'message'. When created: 'path', 'rows', 'missing', 'tokens', 'no_data'.
    """
    set_code = set_code.strip().upper()
    config = config or repricer.load_config()
    cards = scryfall_mirror.cards_in_set(set_code)
    if not cards:
        return {"status": "not_in_mirror",
                "message": f"{set_code} isn't in the Scryfall mirror. Refresh the mirror first "
                           "(TUI [8], or python -m station.scryfall_mirror refresh)."}
    if all(scryfall_mirror.is_token_layout(c["layout"]) for c in cards):
        return {"status": "token_set", "message": f"{set_code} contains only tokens; tokens are handled separately."}

    as_of, products = manapool_client.fetch_single_products(
        scryfall_ids=[c["scryfall_id"] for c in cards if not scryfall_mirror.is_token_layout(c["layout"])])
    product_ids = [v["product_id"] for p in products for v in p.get("variants", [])]
    built = build_rows(set_code, cards, products, foil, config, load_listed(product_ids), repricer.load_pins())
    if not built["rows"]:
        return {"status": "no_products", "missing": built["missing"],
                "message": f"ManaPool has no NM {'foil' if foil else 'non-foil'} products for {set_code}."}

    path = new_sheet_path(set_code, foil)
    os.makedirs(INTAKE_DIR, exist_ok=True)
    write_sheet(path, built["rows"])
    write_meta(path, {"set_code": set_code, "finish": "FO" if foil else "NF", "priced_at": _now_iso(),
                      "feed_as_of": as_of, "prefill": built["prefill"]})
    return {"status": "created", "path": path, **{k: built[k] for k in ("rows", "missing", "tokens", "no_data")}}


# --------------------------------------------------------------------------- files


def _now_iso() -> str:
    return datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")


def meta_path(path: str) -> str:
    """Hidden sidecar next to the sheet: intake/.intake_SOI_2026-11-02.json."""
    stem = os.path.splitext(os.path.basename(path))[0]
    return os.path.join(os.path.dirname(path), f".{stem}.json")


def read_meta(path: str) -> Optional[Dict[str, Any]]:
    """The sheet's sidecar, or None for a sheet this tool did not build (e.g. a website export)."""
    try:
        with open(meta_path(path), encoding="utf-8") as f:
            return json.load(f)
    except (OSError, ValueError):
        return None


def write_meta(path: str, meta: Dict[str, Any]) -> None:
    tmp = meta_path(path) + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(meta, f, indent=1)
    os.replace(tmp, meta_path(path))


def read_sheet(path: str) -> List[Dict[str, str]]:
    with open(path, encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_sheet(path: str, rows: List[Dict[str, str]]) -> None:
    """Writes the sheet atomically, so a crash mid-save never leaves a half-written file."""
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=SHEET_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    os.replace(tmp, path)


def pending_sheets() -> List[str]:
    """CSV files in intake/ waiting to be ingested, newest first."""
    if not os.path.isdir(INTAKE_DIR):
        return []
    paths = [os.path.join(INTAKE_DIR, f) for f in os.listdir(INTAKE_DIR) if f.lower().endswith(".csv")]
    return sorted((p for p in paths if os.path.isfile(p)), key=os.path.getmtime, reverse=True)


def find_unfinished(set_code: str, foil: bool) -> Optional[str]:
    """Newest pending sheet built for this set and finish, if any."""
    for p in pending_sheets():
        m = SHEET_NAME_RE.match(os.path.basename(p))
        if m and m.group(1) == set_code.strip().upper() and bool(m.group(2)) == foil:
            return p
    return None


def is_stale(path: str, now: Optional[datetime.datetime] = None) -> bool:
    """True if the sheet's prices are older than STALE_AFTER (False if it has no sidecar)."""
    meta = read_meta(path)
    if not meta or not meta.get("priced_at"):
        return False
    now = now or datetime.datetime.now(datetime.timezone.utc)
    return now - datetime.datetime.fromisoformat(meta["priced_at"]) > STALE_AFTER


def summarize(rows: List[Dict[str, str]]) -> Dict[str, Any]:
    """Counted lines, cards and dollar value of a sheet."""
    lines = cards = value = 0
    for r in rows:
        qty = int(r.get("quantity") or 0) if str(r.get("quantity") or "").strip().isdigit() else 0
        if qty > 0:
            lines += 1
            cards += qty
            value += qty * price_cents(r.get("price"))
    return {"lines": lines, "cards": cards, "value_cents": value}


def find_row(rows: List[Dict[str, str]], query: str, start: int = 0) -> Optional[int]:
    """Index of the card matching a search: an exact collector number first, else the next
    name containing the text (case- and accent-insensitive) after `start`, wrapping around."""
    q = query.strip()
    for i, r in enumerate(rows):
        if r["number"].lower() == q.lower():
            return i
    needle = name_sort_key(q)
    for step in range(1, len(rows) + 1):
        i = (start + step) % len(rows)
        if needle in name_sort_key(rows[i]["name"]):
            return i
    return None


def refresh_prices(path: str, config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Re-pulls prices for a sheet, keeping quantities and any price edited by hand.

    A row's price is replaced only if it still equals the price this tool pre-filled.
    Market columns are always updated.

    Returns:
        Dict[str, Any]: 'changed' (prices updated), 'kept' (hand-edited prices left alone).

    Raises:
        ValueError: If the sheet has no sidecar (it was not built by this tool).
    """
    meta = read_meta(path)
    if meta is None:
        raise ValueError(f"{os.path.basename(path)} was not built by the intake sheet tool; can't refresh it.")
    config = config or repricer.load_config()
    rows = read_sheet(path)
    as_of, products = manapool_client.fetch_single_products(product_ids=[r["product_id"] for r in rows])
    by_pid = {v["product_id"]: (p, v) for p in products for v in p.get("variants", [])}
    listed = load_listed(list(by_pid))
    pins = repricer.load_pins()
    prefill = meta.setdefault("prefill", {})
    changed = kept = 0
    for r in rows:
        hit = by_pid.get(r["product_id"])
        if not hit:
            continue
        product, variant = hit
        low, market = variant_prices(product, variant)
        r["market_low"] = _money(low)
        r["market_price"] = _money(product.get("price_market"))
        r["market_price_foil"] = _money(product.get("price_market_foil"))
        if r["price"] != prefill.get(r["product_id"]):
            kept += 1
            continue
        price = _money(sheet_price(r["product_id"], low, market, listed.get(r["product_id"]), pins, config)[0])
        changed += price != r["price"]
        r["price"] = prefill[r["product_id"]] = price
    write_sheet(path, rows)
    meta.update({"priced_at": _now_iso(), "feed_as_of": as_of})
    write_meta(path, meta)
    return {"changed": changed, "kept": kept}


def _move(src: str, dest_dir: str) -> str:
    os.makedirs(dest_dir, exist_ok=True)
    stem, ext = os.path.splitext(os.path.basename(src))
    dest = os.path.join(dest_dir, stem + ext)
    n = 2
    while os.path.exists(dest):
        dest = os.path.join(dest_dir, f"{stem}-{n}{ext}")
        n += 1
    shutil.move(src, dest)
    return dest


def retire_sheet(path: str) -> Optional[str]:
    """Moves an ingested sheet (and its sidecar) from intake/ to intake/ingested/.

    Files outside intake/ are left where they are.

    Returns:
        Optional[str]: The sheet's new path, or None if it was not in intake/.
    """
    if os.path.dirname(os.path.abspath(path)) != os.path.abspath(INTAKE_DIR):
        return None
    sidecar = meta_path(path)
    dest = _move(path, ingested_dir())
    if os.path.exists(sidecar):
        shutil.move(sidecar, meta_path(dest))
    return dest


def discard_sheet(path: str) -> None:
    """Deletes a sheet and its sidecar (used when nothing was counted)."""
    for p in (path, meta_path(path)):
        if os.path.exists(p):
            os.remove(p)


# --------------------------------------------------------------------------- CLI


def _print_created(res: Dict[str, Any]) -> None:
    print(f"Wrote {len(res['rows'])} cards to {res['path']}")
    if res["tokens"]:
        print(f"Left out {res['tokens']} token/emblem cards (tokens are handled separately).")
    if res["no_data"]:
        print(f"{res['no_data']} cards have no price data and start at the floor.")
    if res["missing"]:
        print(f"{len(res['missing'])} cards in the mirror are not on the sheet:")
        for m in res["missing"]:
            print(f"  {m['number']:<6} {m['name']}  ({m['reason']})")


def main() -> None:
    parser = argparse.ArgumentParser(description="Build intake sheets (ManaPool catalog CSVs) for one set.")
    sub = parser.add_subparsers(dest="command", required=True)
    p_new = sub.add_parser("new", help="Build a sheet for a set, or resume its unfinished one")
    p_new.add_argument("set_code")
    p_new.add_argument("--foil", action="store_true", help="Foil-only sheet (default: non-foil)")
    p_new.add_argument("--fresh", action="store_true", help="Start a new sheet even if an unfinished one exists")
    p_ref = sub.add_parser("refresh", help="Re-pull prices for a sheet, keeping counts and edited prices")
    p_ref.add_argument("sheet")
    sub.add_parser("list", help="Sheets in intake/ waiting to be ingested")
    args = parser.parse_args()

    if args.command == "new":
        existing = None if args.fresh else find_unfinished(args.set_code, args.foil)
        if existing:
            print(f"Resuming unfinished sheet {existing} (use --fresh to start another).")
            if is_stale(existing):
                res = refresh_prices(existing)
                print(f"Prices were over a day old: refreshed {res['changed']}, kept {res['kept']} edited by hand.")
            return
        res = create_sheet(args.set_code, foil=args.foil)
        if res["status"] != "created":
            print(res["message"])
            for m in res.get("missing", []):
                print(f"  {m['number']:<6} {m['name']}  ({m['reason']})")
            sys.exit(1)
        _print_created(res)

    elif args.command == "refresh":
        res = refresh_prices(args.sheet)
        print(f"Refreshed {res['changed']} prices; kept {res['kept']} edited by hand.")

    elif args.command == "list":
        table = []
        for p in pending_sheets():
            s = summarize(read_sheet(p))
            meta = read_meta(p) or {}
            table.append([os.path.basename(p), s["lines"], s["cards"], f"${s['value_cents'] / 100:.2f}",
                          (meta.get("priced_at") or "-")[:16]])
        print(tabulate(table, headers=["Sheet", "Lines", "Cards", "Value", "Priced (UTC)"])
              if table else "No sheets waiting in intake/.")


if __name__ == "__main__":
    main()
