"""Fulfillment pull list optimizer module.

Ingests ManaPool order picklists (either via API or picklist CSV exports),
resolves physical storage coordinates (Box -> Set -> Section -> Index),
sorts the pick sequence to eliminate warehouse backtracking,
generates clipboard-ready printable pick sheets with [ ] checkboxes,
and decrements inventory upon fulfillment.
"""

import argparse
import csv
import datetime
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

from tabulate import tabulate

from station import db
from station import manapool_client
from station import scryfall_mirror


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
) -> str:
    """Exports a formatted printable pick sheet with [ ] checkboxes for clipboard use.

    Args:
        allocated (List[Dict[str, Any]]): Sorted allocated picks.
        unallocated (List[Dict[str, Any]]): Missing or out-of-stock items.
        output_dir (str): Destination folder.
        source_label (str): Label for header.

    Returns:
        str: Absolute path to the generated pick sheet.
    """
    os.makedirs(output_dir, exist_ok=True)
    today_str = datetime.date.today().isoformat()
    filename = f"pick_sheet_{today_str}.txt"
    filepath = os.path.join(output_dir, filename)

    total_cards = sum(p["qty_to_pull"] for p in allocated)

    header = f"""========================================================================================
                     MANAPOOL FULFILLMENT OPTIMIZED PICK SHEET
========================================================================================
 Date:       {today_str}
 Source:     {source_label}
 Total:      {total_cards} cards to pull across {len(allocated)} pick steps
 Path:       Sorted by Box -> Set -> Section -> Index (token chunks: Box -> Section -> Index)
========================================================================================
"""
    table_rows = []
    for seq, p in enumerate(allocated, start=1):
        foil_str = "FOIL" if p["is_foil"] else "NF"
        table_rows.append([
            f"#{seq:02d}",
            p["box_label"],
            p["set_code"],
            p["section_label"],
            f"#{p['index_num']:03d}",
            p["name"][:30],
            p["collector_number"],
            foil_str,
            p["qty_to_pull"],
            p["order_label"],
            "[   ]",
        ])

    table_text = tabulate(
        table_rows,
        headers=["Seq", "Box", "Set", "Section", "Index", "Card Name", "Num", "Finish", "Qty", "Order", "Done"],
        tablefmt="simple",
    )

    unallocated_text = ""
    if unallocated:
        unallocated_text = "\n\n========================================================================================\n"
        unallocated_text += " [!] OUT OF STOCK / UNALLOCATED ITEMS (REQUIRES ATTENTION):\n"
        unallocated_text += "========================================================================================\n"
        un_rows = []
        for u in unallocated:
            foil_str = "FOIL" if u["is_foil"] else "NF"
            un_rows.append([
                u["Description"][:35],
                u["Set"],
                u["Number"],
                foil_str,
                u["missing_qty"],
                u.get("order_label", ""),
            ])
        unallocated_text += tabulate(
            un_rows,
            headers=["Card Name", "Set", "Num", "Finish", "Missing Qty", "Order"],
            tablefmt="simple",
        )

    with open(filepath, "w", encoding="utf-8") as f:
        f.write(header)
        f.write(table_text)
        if unallocated_text:
            f.write(unallocated_text)
        f.write("\n========================================================================================\n")

    return os.path.abspath(filepath)


def commit_pick_deductions(allocated: List[Dict[str, Any]]) -> int:
    """Decrements inventory quantities in PostgreSQL for picked items.

    Args:
        allocated (List[Dict[str, Any]]): List of allocated pick records.

    Returns:
        int: Number of inventory lines updated.
    """
    if not allocated:
        return 0

    query = """
        UPDATE inventory
        SET quantity = GREATEST(0, quantity - %s)
        WHERE inventory_id = %s;
    """
    params = [(p["qty_to_pull"], p["inventory_id"]) for p in allocated]

    with db.get_connection() as conn:
        with conn.cursor() as cur:
            from psycopg2 import extras
            extras.execute_batch(cur, query, params)
        conn.commit()

    return len(allocated)


def process_pick_routine(
    source_type: str = "csv",
    csv_file: Optional[str] = None,
    commit: bool = False,
    output_dir: str = "exports",
) -> Dict[str, Any]:
    """Runs the complete pick list optimization routine from CSV or ManaPool API.

    Args:
        source_type (str): 'csv' or 'api'.
        csv_file (Optional[str]): Path to picklist CSV if source_type == 'csv'.
        commit (bool): If True, decrements stock in PostgreSQL.
        output_dir (str): Folder for pick sheet output.

    Returns:
        Dict[str, Any]: Routine summary dictionary.
    """
    if source_type == "api":
        if not manapool_client.is_configured():
            return {
                "status": "error",
                "message": "ManaPool API credentials not configured in .env.",
            }
        pick_items, errors = manapool_client.fetch_unfulfilled_orders()
        if not pick_items:
            return {
                "status": "empty",
                "message": "No unfulfilled orders found on ManaPool or API error occurred.",
                "errors": errors,
            }
        source_label = "ManaPool Live API Orders"
    else:
        if not csv_file:
            return {"status": "error", "message": "No picklist CSV file specified."}
        pick_items = read_picklist_csv(csv_file)
        source_label = os.path.basename(csv_file)

    allocated, unallocated = resolve_and_optimize_picks(pick_items)

    sheet_path = generate_pick_sheet_file(
        allocated=allocated,
        unallocated=unallocated,
        output_dir=output_dir,
        source_label=source_label,
    )

    committed_count = 0
    if commit and allocated:
        committed_count = commit_pick_deductions(allocated)

    return {
        "status": "success",
        "total_requested": len(pick_items),
        "total_allocated": len(allocated),
        "total_cards": sum(p["qty_to_pull"] for p in allocated),
        "unallocated_count": len(unallocated),
        "allocated_items": allocated,
        "unallocated_items": unallocated,
        "sheet_path": sheet_path,
        "committed": commit,
        "committed_count": committed_count,
    }


def main():
    """CLI entry point for pull_list_optimizer."""
    parser = argparse.ArgumentParser(description="MTG ManaPool Order Pull List Optimizer")
    parser.add_argument("picklist_csv", nargs="?", default=None, help="Path to ManaPool picklist CSV file")
    parser.add_argument("--api", action="store_true", help="Fetch unfulfilled orders directly from ManaPool API")
    parser.add_argument("--commit", action="store_true", help="Commit deductions to PostgreSQL inventory")
    parser.add_argument("--output-dir", default="exports", help="Directory for generated pick sheet")

    args = parser.parse_args()

    if args.api:
        print("Fetching unfulfilled orders from ManaPool API...")
        res = process_pick_routine(source_type="api", commit=args.commit, output_dir=args.output_dir)
    elif args.picklist_csv:
        print(f"Processing picklist {args.picklist_csv}...")
        res = process_pick_routine(source_type="csv", csv_file=args.picklist_csv, commit=args.commit, output_dir=args.output_dir)
    else:
        parser.print_help()
        sys.exit(1)

    if res.get("status") == "success":
        print(f"\nSuccessfully optimized pick sequence!")
        print(f"Cards to Pull: {res['total_cards']} across {res['total_allocated']} pick steps.")
        print(f"Printable Pick Sheet: {res['sheet_path']}")
        if res["unallocated_count"] > 0:
            print(f"WARNING: {res['unallocated_count']} items could not be allocated from stock.")
        if res["committed"]:
            print(f"Inventory Deducted: {res['committed_count']} lines updated in PostgreSQL.")
        else:
            print("Note: Inventory was not decremented. Pass --commit to deduct stock.")
    else:
        print(f"Error: {res.get('message')}")


if __name__ == "__main__":
    main()
