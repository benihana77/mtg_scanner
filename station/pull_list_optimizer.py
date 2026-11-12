"""Fulfillment pull list optimizer module.

Ingests ManaPool order picklists (either via API or picklist CSV exports),
resolves physical storage coordinates (Box -> Set -> Section -> Index),
sorts the pick sequence to eliminate warehouse backtracking,
generates clipboard-ready printable pick sheets with [ ] checkboxes,
and decrements inventory upon fulfillment.

Live API orders are deducted at most once: each deducted order is recorded in the
`picked_orders` ledger in the same transaction, and recorded orders are left off later
sheets and deductions. Picklist CSVs carry no order IDs and are not recorded.
"""

import argparse
import csv
import datetime
import os
import re
import sys
import textwrap
from typing import Any, Dict, Iterable, List, Optional, Tuple

from psycopg2 import extras

from station import db
from station import manapool_client
from station import scryfall_mirror

SHEET_WIDTH = 80  # Characters per printed line (plain text on a Letter/A4 page)

LEDGER_MISSING = ("Order ledger table picked_orders is missing: run `python -m station.db` once. "
                  "Until then already-deducted orders cannot be detected and deducting live orders is refused.")
CSV_NOT_LEDGERED = ("Picklist CSVs carry no ManaPool order IDs, so a deduction from one is NOT recorded in the "
                    "order ledger: deducting the same picklist again, or deducting its orders later with the "
                    "live API pick, removes the stock twice.")


def read_picklist_csv(csv_path: str) -> List[Dict[str, Any]]:
    """Reads a ManaPool picklist CSV export file.

    Expected headers: Qty,Description,Set,Condition,Finish,Lang,Number

    Args:
        csv_path (str): Path to picklist CSV.

    Returns:
        List[Dict[str, Any]]: List of pick items.
    """
    if not os.path.isfile(csv_path):
        raise FileNotFoundError(f"Picklist file not found: {csv_path}")

    items = []
    with open(csv_path, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=1):
            try:
                qty = int(row.get("Qty", 1) or 1)
            except ValueError:
                qty = 1

            finish = (row.get("Finish") or "NF").strip().upper()
            is_foil = (finish in ["FO", "FOIL"])

            items.append({
                "item_id": idx,
                "Qty": qty,
                "Description": (row.get("Description") or "").strip(),
                "Set": (row.get("Set") or "").strip().upper(),
                "Condition": (row.get("Condition") or "NM").strip(),
                "Finish": finish,
                "is_foil": is_foil,
                "Lang": (row.get("Lang") or "EN").strip(),
                "Number": str(row.get("Number") or "").strip(),
                "order_label": row.get("order_label", f"Order-{idx}"),
            })
    return items


def resolve_and_optimize_picks(
    pick_items: List[Dict[str, Any]],
) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    """Matches requested pick items to PostgreSQL physical inventory and sorts by traversal path.

    Order lines are resolved through the Scryfall mirror exactly like intake (so ManaPool
    double-sided token combos such as 'TMID 8-11' match their front + back faces) and
    matched on (scryfall_id, back_scryfall_id, foil). If a line cannot be resolved, it falls
    back to set code + card name.

    Traversal sort order:
        regular sections: box_label -> set_code -> section_label -> index_num
        token chunks (TOKEN-n): box_label -> section_label -> index_num (set code ignored)

    Args:
        pick_items (List[Dict[str, Any]]): List of items to pick.

    Returns:
        Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]: (allocated_picks, unallocated_picks).
    """
    allocated: List[Dict[str, Any]] = []
    unallocated: List[Dict[str, Any]] = []

    # Query all available inventory joined with cards
    query = """
        SELECT
            i.inventory_id,
            i.scryfall_id,
            i.back_scryfall_id,
            sc.name || COALESCE(' // ' || b.name, '') AS name,
            sc.set_code,
            sc.collector_number || COALESCE('/' || b.collector_number, '') AS collector_number,
            i.condition,
            i.is_foil,
            i.quantity,
            i.manapool_product_id,
            i.box_label,
            i.section_label,
            i.index_num
        FROM inventory i
        JOIN scryfall_cards sc ON i.scryfall_id = sc.scryfall_id
        LEFT JOIN scryfall_cards b ON i.back_scryfall_id = b.scryfall_id
        WHERE i.quantity > 0
        ORDER BY i.box_label, sc.set_code, i.section_label, i.index_num;
    """
    with db.get_db_cursor(commit=False) as cur:
        cur.execute(query)
        db_inventory = [dict(r) for r in cur.fetchall()]

    # Sort candidates strictly by FIFO priority:
    # Box -> Set -> Section (Sec-1 before Sec-2 before Sec-3) -> Index
    def candidate_fifo_key(item: Dict[str, Any]):
        box = item.get("box_label", "")
        m_box = re.search(r"(\d+)", box)
        box_num = int(m_box.group(1)) if m_box else 999

        set_code = (item.get("set_code") or "").upper()

        sec = item.get("section_label", "")
        m_sec = re.search(r"(\d+)", sec)
        sec_num = int(m_sec.group(1)) if m_sec else 999

        return (box_num, box, set_code, sec_num, sec, item.get("index_num", 0))

    db_inventory.sort(key=candidate_fifo_key)

    resolved = scryfall_mirror.resolve_manapool_cards((p["Set"], p["Number"]) for p in pick_items)

    # In-memory allocation pool so multiple order items for the same card decrement properly
    stock_pool = {item["inventory_id"]: item["quantity"] for item in db_inventory}
    inv_lookup = {item["inventory_id"]: item for item in db_inventory}

    for order_line in pick_items:
        needed_qty = order_line["Qty"]
        set_code = order_line["Set"].upper()
        num = order_line["Number"]
        is_foil = order_line["is_foil"]
        name = order_line["Description"]
        res = resolved.get((set_code, num.strip().lower()))
        want_ids = (res["front"]["scryfall_id"], res["back"]["scryfall_id"] if res["back"] else None) if res else None

        # Find matching inventory candidates (FIFO: Sec-1 before Sec-2)
        candidates = []
        for item in db_inventory:
            inv_id = item["inventory_id"]
            if stock_pool.get(inv_id, 0) <= 0:
                continue

            # Match criteria: resolved Scryfall card (front + back face) and foil flag
            match_exact = (
                want_ids is not None
                and (item["scryfall_id"], item["back_scryfall_id"]) == want_ids
                and item["is_foil"] == is_foil
            )
            # Fallback criteria: set + card name when the order line could not be resolved
            match_name = (
                want_ids is None
                and item["set_code"].upper() == set_code
                and item["name"].strip().lower() == name.strip().lower()
                and item["is_foil"] == is_foil
            )

            if match_exact or match_name:
                candidates.append(item)

        # Allocate quantities across candidates
        qty_remaining = needed_qty
        for cand in candidates:
            inv_id = cand["inventory_id"]
            available = stock_pool[inv_id]
            take = min(available, qty_remaining)

            allocated.append({
                "inventory_id": inv_id,
                "box_label": cand["box_label"],
                "section_label": cand["section_label"],
                "index_num": cand["index_num"],
                "name": cand["name"],
                "set_code": cand["set_code"].upper(),
                "collector_number": cand["collector_number"],
                "is_foil": cand["is_foil"],
                "condition": cand["condition"],
                "qty_to_pull": take,
                "manapool_product_id": cand["manapool_product_id"],
                "order_id": order_line.get("order_id"),
                "order_label": order_line.get("order_label", ""),
            })

            stock_pool[inv_id] -= take
            qty_remaining -= take
            if qty_remaining == 0:
                break

        if qty_remaining > 0:
            unallocated.append({
                **order_line,
                "missing_qty": qty_remaining,
            })

    # Sort allocated picks strictly by physical warehouse hierarchy:
    # 1. Box: Natural numerical order (M1 -> M2 ... -> M19)
    # 2. Set: Alphabetical set code (e.g. BRO, FEM, MID, NEO); token chunks use 'TOKENS'
    #    so a TOKEN-n chunk is walked straight down its index across its many sets
    # 3. Section: Natural section order (Sec-1 -> Sec-2 -> Sec-3)
    # 4. Card Name / Index: Alphabetical by card name / sequential index in that section
    def pick_traversal_key(x: Dict[str, Any]):
        box = x.get("box_label", "")
        m_box = re.search(r"(\d+)", box)
        box_num = int(m_box.group(1)) if m_box else 999

        sec = x.get("section_label", "")
        set_code = db.TOKENS_SET if sec.upper().startswith("TOKEN") else (x.get("set_code") or "").upper()

        m_sec = re.search(r"(\d+)", sec)
        sec_num = int(m_sec.group(1)) if m_sec else 999

        card_name = (x.get("name") or "").lower()
        index_num = x.get("index_num", 0)

        return (box_num, box, set_code, sec_num, sec, index_num, card_name)

    allocated.sort(key=pick_traversal_key)

    return allocated, unallocated


def generate_pick_sheet_file(
    allocated: List[Dict[str, Any]],
    unallocated: List[Dict[str, Any]],
    output_dir: str = "exports",
    source_label: str = "ManaPool",
    already_picked: Optional[List[Dict[str, Any]]] = None,
    notes: Optional[List[str]] = None,
    orders: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Exports a printable pick sheet with [ ] checkboxes, at most SHEET_WIDTH characters wide.

    Each order is shown by its short key (A, B, ...) with a key-to-label list in the header,
    so rows fit one printed line; card names are cut to the room that is left.

    Args:
        allocated (List[Dict[str, Any]]): Sorted allocated picks.
        unallocated (List[Dict[str, Any]]): Missing or out-of-stock items.
        output_dir (str): Destination folder.
        source_label (str): Label for header.
        already_picked (Optional[List[Dict[str, Any]]]): Orders left off because their stock
            was already deducted (order_label, picked_at).
        notes (Optional[List[str]]): Warnings printed under the header.
        orders (Optional[List[Dict[str, Any]]]): `summarize_orders` output (live API orders).
            None or empty leaves out the order column (picklist CSV lines have no real orders).

    Returns:
        str: Absolute path to the generated pick sheet.
    """
    os.makedirs(output_dir, exist_ok=True)
    today_str = datetime.date.today().isoformat()
    filename = f"pick_sheet_{today_str}.txt"
    filepath = os.path.join(output_dir, filename)

    total_cards = sum(p["qty_to_pull"] for p in allocated)
    keys = {o["order_label"]: o["key"] for o in orders or []}
    rule = "=" * SHEET_WIDTH

    lines = [
        rule,
        "MANAPOOL FULFILLMENT PICK SHEET".center(SHEET_WIDTH).rstrip(),
        rule,
        f" Date:    {today_str}",
        f" Source:  {source_label}",
        f" Total:   {total_cards} cards to pull across {len(allocated)} pick steps",
        " Path:    Box > Set > Section > Index (token chunks: Box > Section > Index)",
    ]
    key_width = max((len(k) for k in keys.values()), default=0)
    for n, o in enumerate(orders or []):
        counts = f"{o['cards']} cards" + (f", {o['missing']} MISSING" if o["missing"] else "")
        lines.append(f" {'Orders:' if n == 0 else '':<8} {o['key']:<{key_width}}  {o['order_label']}  {counts}")
    lines.append(rule)
    for note in notes or []:
        lines += textwrap.wrap(note, SHEET_WIDTH, initial_indent=" [!] ", subsequent_indent="     ")
    if notes:
        lines.append(rule)

    finish = lambda foil: "FOIL" if foil else "NF"
    order_col = ["Ord"] if keys else []
    table_rows = [
        [p["box_label"], p["set_code"], p["section_label"], f"{p['index_num']:03d}", p["name"],
         p["collector_number"], finish(p["is_foil"]), p["qty_to_pull"]]
        + ([keys.get(p["order_label"], "")] if keys else []) + ["[ ]"]
        for p in allocated
    ]
    lines += _text_table(
        ["Box", "Set", "Sec", "Idx", "Card Name", "Num", "Fin", "Qty"] + order_col + ["Done"],
        table_rows, name_col=4, right={3, 5, 7},
    )

    if unallocated:
        lines += ["", rule, " [!] OUT OF STOCK / NOT ALLOCATED (needs attention):", rule]
        lines += _text_table(
            ["Card Name", "Set", "Num", "Fin", "Missing"] + order_col,
            [[u["Description"], u["Set"], u["Number"], finish(u["is_foil"]), u["missing_qty"]]
             + ([keys.get(u.get("order_label"), "")] if keys else []) for u in unallocated],
            name_col=0, right={2, 4},
        )

    if already_picked:
        lines += ["", rule, " ALREADY PICKED: deducted earlier, not on this sheet (ship, then mark shipped)", rule]
        lines += _text_table(
            ["Order", "Deducted at"],
            [[o["order_label"], f"{o['picked_at']:%Y-%m-%d %H:%M}"] for o in already_picked],
        )

    lines.append(rule)
    with open(filepath, "w", encoding="utf-8") as f:
        f.write("\n".join(lines) + "\n")

    return os.path.abspath(filepath)


def _text_table(
    headers: List[str],
    rows: List[List[Any]],
    name_col: Optional[int] = None,
    right: Iterable[int] = (),
) -> List[str]:
    """Plain-text table lines: columns as wide as their widest cell, two spaces apart.

    The `name_col` column gets whatever is left of SHEET_WIDTH and longer values are cut,
    so the table fits a printed page. Columns in `right` are right-aligned.
    """
    cells = [[str(c) for c in r] for r in rows]
    widths = [max([len(h)] + [len(r[i]) for r in cells]) for i, h in enumerate(headers)]
    if name_col is not None:
        room = SHEET_WIDTH - 2 * (len(headers) - 1) - sum(w for i, w in enumerate(widths) if i != name_col)
        widths[name_col] = max(len(headers[name_col]), min(widths[name_col], room))
    right = set(right)

    def line(row: List[str]) -> str:
        return "  ".join(
            c[:w].rjust(w) if i in right else c[:w].ljust(w) for i, (c, w) in enumerate(zip(row, widths))
        ).rstrip()

    return [line(headers), "  ".join("-" * w for w in widths)] + [line(r) for r in cells]


def ledger_exists() -> bool:
    """Whether the `picked_orders` ledger table exists (created by `python -m station.db`)."""
    with db.get_db_cursor(commit=False) as cur:
        cur.execute("SELECT to_regclass('picked_orders') IS NOT NULL AS ok;")
        return bool(cur.fetchone()["ok"])


def picked_orders(order_ids: Iterable[str]) -> Dict[str, Dict[str, Any]]:
    """Ledger entries for the given ManaPool order IDs (orders whose stock was already deducted).

    Args:
        order_ids (Iterable[str]): ManaPool order UUIDs.

    Returns:
        Dict[str, Dict[str, Any]]: order_id -> {'order_label', 'picked_at'}. Empty if no IDs are
            given or the ledger table does not exist yet.
    """
    ids = sorted({str(o) for o in order_ids if o})
    if not ids or not ledger_exists():
        return {}
    with db.get_db_cursor(commit=False) as cur:
        cur.execute("SELECT order_id, order_label, picked_at FROM picked_orders WHERE order_id = ANY(%s);", (ids,))
        return {r["order_id"]: {"order_label": r["order_label"], "picked_at": r["picked_at"]} for r in cur.fetchall()}


def commit_pick_deductions(
    allocated: List[Dict[str, Any]],
    unallocated: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Decrements inventory quantities in PostgreSQL for picked items, recording each order once.

    One transaction. Picks that carry an order_id (live API orders) are deducted only if their
    order can be added to the `picked_orders` ledger; an order already there is skipped, so it
    is never deducted twice, even by two runs at once. An order with nothing allocated is not
    recorded. Picks without an order_id (picklist CSVs, personal pulls) are deducted with no
    ledger entry.

    Args:
        allocated (List[Dict[str, Any]]): List of allocated pick records.
        unallocated (Optional[List[Dict[str, Any]]]): Order lines not found in stock, stored with
            their order's ledger entry.

    Returns:
        Dict[str, Any]: 'lines_updated' (inventory rows decremented), 'orders_recorded' and
            'orders_skipped' (labels of orders deducted now / already in the ledger).
    """
    result: Dict[str, Any] = {"lines_updated": 0, "orders_recorded": [], "orders_skipped": []}
    if not allocated:
        return result

    orders: Dict[str, Dict[str, Any]] = {}
    for p in allocated:
        if p.get("order_id"):
            o = orders.setdefault(p["order_id"], {"label": p.get("order_label") or p["order_id"], "lines": [], "missing": []})
            o["lines"].append({
                "inventory_id": p["inventory_id"], "product_id": p.get("manapool_product_id"), "qty": p["qty_to_pull"],
                "name": p["name"], "set": p["set_code"], "number": p["collector_number"], "foil": p["is_foil"],
                "location": f"{p['box_label']} {p['section_label']} #{p['index_num']:03d}",
            })
    for u in unallocated or []:
        if u.get("order_id") in orders:
            orders[u["order_id"]]["missing"].append({
                "name": u["Description"], "set": u["Set"], "number": u["Number"], "foil": u["is_foil"],
                "missing_qty": u["missing_qty"], "product_id": u.get("manapool_product_id"),
            })

    recorded = set()
    now = datetime.datetime.now()  # station's local clock (the database server runs on UTC)
    with db.get_db_cursor() as cur:
        for order_id, o in orders.items():
            cur.execute("""
                INSERT INTO picked_orders (order_id, order_label, picked_at, lines, missing)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (order_id) DO NOTHING
                RETURNING order_id;
            """, (order_id, o["label"], now, extras.Json(o["lines"]), extras.Json(o["missing"])))
            if cur.fetchone():
                recorded.add(order_id)
        rows = [p for p in allocated if not p.get("order_id") or p["order_id"] in recorded]
        extras.execute_batch(cur, """
            UPDATE inventory
            SET quantity = GREATEST(0, quantity - %s)
            WHERE inventory_id = %s;
        """, [(p["qty_to_pull"], p["inventory_id"]) for p in rows])

    result["lines_updated"] = len(rows)
    result["orders_recorded"] = [o["label"] for oid, o in orders.items() if oid in recorded]
    result["orders_skipped"] = [o["label"] for oid, o in orders.items() if oid not in recorded]
    return result


def summarize_orders(
    allocated: List[Dict[str, Any]],
    unallocated: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Per-order totals for a sheet, in first-seen (walk) order.

    Each entry has the order's short sheet key ('A', 'B', ... 'Z', 'AA', ...), order_label,
    cards to pull and cards missing.
    """
    orders: Dict[str, Dict[str, Any]] = {}
    for p in allocated:
        orders.setdefault(p["order_label"], {"order_label": p["order_label"], "cards": 0, "missing": 0})["cards"] += p["qty_to_pull"]
    for u in unallocated:
        label = u.get("order_label", "")
        orders.setdefault(label, {"order_label": label, "cards": 0, "missing": 0})["missing"] += u["missing_qty"]
    return [{"key": order_key(n), **o} for n, o in enumerate(orders.values())]


def order_key(n: int) -> str:
    """Short sheet key for the n-th order (0-based): A..Z, then AA, AB, ..."""
    key = ""
    n += 1
    while n:
        n, r = divmod(n - 1, 26)
        key = chr(ord("A") + r) + key
    return key


def commit_sheet(res: Dict[str, Any], order_labels: Optional[Iterable[str]] = None) -> Dict[str, Any]:
    """Deducts the picks of a sheet built by `process_pick_routine`, optionally only some orders.

    Deducts exactly what the sheet showed, so an order that arrives after the sheet was
    printed is not deducted by accident.

    Args:
        res (Dict[str, Any]): A successful `process_pick_routine` result.
        order_labels (Optional[Iterable[str]]): Only these orders (sheet key such as 'A', label
            or order ID); None = all.

    Returns:
        Dict[str, Any]: `res` updated with committed, committed_count, orders_deducted, orders_skipped.

    Raises:
        RuntimeError: For live API orders when the ledger table does not exist.
    """
    if res["source"] == "api" and not res["ledgered"]:
        raise RuntimeError(LEDGER_MISSING)
    allocated, unallocated = res["allocated_items"], res["unallocated_items"]
    if order_labels is not None:
        by_key = {o["key"]: o["order_label"] for o in res.get("orders", [])}
        wanted = {by_key.get(str(o).strip().upper(), str(o).strip()) for o in order_labels}
        allocated = [p for p in allocated if p["order_label"] in wanted or p.get("order_id") in wanted]
        unallocated = [u for u in unallocated if u.get("order_label") in wanted or u.get("order_id") in wanted]
    done = commit_pick_deductions(allocated, unallocated)
    res.update({
        "committed": True,
        "committed_count": done["lines_updated"],
        "orders_deducted": done["orders_recorded"],
        "orders_skipped": done["orders_skipped"],
    })
    return res


def process_pick_routine(
    source_type: str = "csv",
    csv_file: Optional[str] = None,
    commit: bool = False,
    output_dir: str = "exports",
    order_labels: Optional[Iterable[str]] = None,
) -> Dict[str, Any]:
    """Runs the complete pick list optimization routine from CSV or ManaPool API.

    Live API orders already in the `picked_orders` ledger are left off the sheet and never
    deducted again; they are listed in 'already_picked'.

    Args:
        source_type (str): 'csv' or 'api'.
        csv_file (Optional[str]): Path to picklist CSV if source_type == 'csv'.
        commit (bool): If True, decrements stock in PostgreSQL (and records API orders).
        output_dir (str): Folder for pick sheet output.
        order_labels (Optional[Iterable[str]]): Only these orders (label or order ID); None = all.

    Returns:
        Dict[str, Any]: Routine summary dictionary. 'status' is 'success', 'empty' or 'error'.
    """
    notes: List[str] = []
    already_picked: List[Dict[str, Any]] = []
    ledgered = False
    if source_type == "api":
        if not manapool_client.is_configured():
            return {
                "status": "error",
                "message": "ManaPool API credentials not configured in .env.",
            }
        ledgered = ledger_exists()
        if commit and not ledgered:
            return {"status": "error", "message": LEDGER_MISSING}
        pick_items, errors = manapool_client.fetch_unfulfilled_orders()
        if errors and not pick_items:
            return {"status": "error", "message": errors[0], "errors": errors}
        notes += [f"Incomplete order list: {e}" for e in errors]
        if not ledgered:
            notes.append(LEDGER_MISSING)
        done = picked_orders(it["order_id"] for it in pick_items) if ledgered else {}
        already_picked = sorted(({"order_id": oid, **d} for oid, d in done.items()), key=lambda o: o["picked_at"])
        pick_items = [it for it in pick_items if it["order_id"] not in done]
        source_label = "ManaPool Live API Orders"
    else:
        if not csv_file:
            return {"status": "error", "message": "No picklist CSV file specified."}
        pick_items = read_picklist_csv(csv_file)
        source_label = os.path.basename(csv_file)
        notes.append(CSV_NOT_LEDGERED)

    if order_labels is not None:
        wanted = {str(o).strip() for o in order_labels}
        pick_items = [it for it in pick_items if it.get("order_label") in wanted or it.get("order_id") in wanted]
    if not pick_items:
        if already_picked:
            message = f"Nothing to pick: {len(already_picked)} order(s) waiting to ship were already picked and deducted."
        elif source_type == "api":
            message = "No orders waiting to ship on ManaPool" + (" match the given labels." if order_labels else ".")
        else:
            message = "The picklist has no lines" + (" for the given orders." if order_labels else ".")
        return {"status": "empty", "message": message, "already_picked": already_picked, "notes": notes}

    allocated, unallocated = resolve_and_optimize_picks(pick_items)
    orders = summarize_orders(allocated, unallocated) if source_type == "api" else []

    sheet_path = generate_pick_sheet_file(
        allocated=allocated,
        unallocated=unallocated,
        output_dir=output_dir,
        source_label=source_label,
        already_picked=already_picked,
        notes=notes,
        orders=orders,
    )

    res: Dict[str, Any] = {
        "status": "success",
        "source": source_type,
        "total_requested": len(pick_items),
        "total_allocated": len(allocated),
        "total_cards": sum(p["qty_to_pull"] for p in allocated),
        "unallocated_count": len(unallocated),
        "allocated_items": allocated,
        "unallocated_items": unallocated,
        "orders": orders,
        "already_picked": already_picked,
        "ledgered": ledgered,
        "notes": notes,
        "sheet_path": sheet_path,
        "committed": False,
        "committed_count": 0,
        "orders_deducted": [],
        "orders_skipped": [],
    }
    if commit and allocated:
        commit_sheet(res)
    return res


def main():
    """CLI entry point for pull_list_optimizer."""
    parser = argparse.ArgumentParser(description="MTG ManaPool Order Pull List Optimizer")
    parser.add_argument("picklist_csv", nargs="?", default=None, help="Path to ManaPool picklist CSV file")
    parser.add_argument("--api", action="store_true", help="Fetch orders waiting to ship directly from ManaPool API")
    parser.add_argument("--commit", action="store_true", help="Commit deductions to PostgreSQL inventory")
    parser.add_argument("--order", dest="orders", action="append",
                        help="Only this order label (repeatable); default every order waiting to ship")
    parser.add_argument("--output-dir", default="exports", help="Directory for generated pick sheet")

    args = parser.parse_args()

    if args.api:
        print("Fetching orders waiting to ship from ManaPool API...")
        res = process_pick_routine(source_type="api", commit=args.commit, output_dir=args.output_dir,
                                   order_labels=args.orders)
    elif args.picklist_csv:
        print(f"Processing picklist {args.picklist_csv}...")
        res = process_pick_routine(source_type="csv", csv_file=args.picklist_csv, commit=args.commit,
                                   output_dir=args.output_dir, order_labels=args.orders)
    else:
        parser.print_help()
        sys.exit(1)

    for o in res.get("already_picked", []):
        print(f"Already picked (deducted {o['picked_at']:%Y-%m-%d %H:%M}), skipped: {o['order_label']}")
    for note in res.get("notes", []):
        print(f"[!] {note}", file=sys.stderr)

    if res.get("status") == "success":
        print(f"\nSuccessfully optimized pick sequence!")
        for o in res["orders"]:
            print(f"  {o['key']}  Order {o['order_label']}: {o['cards']} cards" + (f", {o['missing']} MISSING" if o["missing"] else ""))
        print(f"Cards to Pull: {res['total_cards']} across {res['total_allocated']} pick steps.")
        print(f"Printable Pick Sheet: {res['sheet_path']}")
        if res["unallocated_count"] > 0:
            print(f"WARNING: {res['unallocated_count']} items could not be allocated from stock.")
        if res["committed"]:
            print(f"Inventory Deducted: {res['committed_count']} lines updated in PostgreSQL.")
            if res["orders_deducted"]:
                print(f"Recorded as picked: {', '.join(res['orders_deducted'])}")
            if res["orders_skipped"]:
                print(f"Skipped, already picked: {', '.join(res['orders_skipped'])}")
        else:
            print("Note: Inventory was not decremented. Pass --commit to deduct stock.")
    elif res.get("status") == "empty":
        print(res["message"])
    else:
        print(f"Error: {res.get('message')}")


if __name__ == "__main__":
    main()
