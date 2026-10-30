"""ManaPool CSV ingestion, indexing, and physical storage pipeline.

Handles:
- Reading ManaPool set inventory spreadsheets.
- Filtering active batches (quantity > 0, singles only).
- Resolving cards against the local Scryfall mirror (no live Scryfall calls), including
  ManaPool double-sided token combos.
- Routing tokens (by Scryfall layout) from every set in the batch into one new TOKEN-n chunk.
- Auto-allocating physical section identifiers (Sec-1, Sec-2, etc.).
- Shelf-order sorting within sections and sequential index numbers (1, 2, 3... N).
- Committing to the PostgreSQL inventory table.
- Generating ManaPool upload CSVs.
- Relocating sets/sections (or all TOKENS) across boxes when capacity runs out.
"""

import argparse
import csv
import datetime
import os
import sys
from typing import Any, Dict, List, Optional, Tuple

from tabulate import tabulate

from station import db
from station import import_live_inventory
from station import manapool_client
from station import scryfall_mirror
from station import SET_BOX_MAP_CSV


def read_manapool_csv(file_path: str) -> List[Dict[str, Any]]:
    """Reads a ManaPool set CSV file and filters rows where quantity > 0.

    Args:
        file_path (str): Path to input CSV.

    Returns:
        List[Dict[str, Any]]: List of row dictionaries with positive quantities.
    """
    if not os.path.isfile(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    active_rows: List[Dict[str, Any]] = []
    with open(file_path, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                qty = int(row.get("quantity", 0) or 0)
            except ValueError:
                qty = 0

            if qty > 0:
                active_rows.append(row)

    return active_rows


def resolve_set_box(
    set_code: str,
    interactive: bool = True,
    prompt_callback: Optional[Any] = None,
) -> str:
    """Resolves the physical storage box for a shelf set (an MTG set code or 'TOKENS').

    1. Checks if the shelf set already exists in PostgreSQL inventory.
    1b. For 'TOKENS' only, falls back to the '(TOKENS)' row of the Set/Box map.
    2. If still not found, the set does not currently exist in inventory:
       a. Calls prompt_callback(set_code) if provided.
       b. Prompts the operator via stdin if interactive and a TTY.
       c. If no box is provided, raises ValueError.

    Args:
        set_code (str): MTG set code (e.g. 'MID', 'HOB').
        interactive (bool): Whether to prompt the user via stdin if unmapped.
        prompt_callback (Optional[Any]): Callback function to prompt user in custom UI.

    Returns:
        str: Physical box identifier (e.g. 'M2').
    """
    set_code = set_code.strip().upper()

    # 1. Check existing PostgreSQL inventory
    if set_code == db.TOKENS_SET:
        token_box = db.get_token_box()
        if token_box:
            return token_box.strip()
        mapped = import_live_inventory.load_set_box_map(SET_BOX_MAP_CSV).get(db.TOKENS_SET)
        if mapped:
            return mapped
    else:
        query = f"""
            SELECT DISTINCT i.box_label
            FROM inventory i
            JOIN scryfall_cards sc ON i.scryfall_id = sc.scryfall_id
            WHERE {db.SHELF_SET_SQL} = %s
            LIMIT 1;
        """
        with db.get_db_cursor(commit=False) as cur:
            cur.execute(query, (set_code,))
            row = cur.fetchone()
            if row and row.get("box_label"):
                return row["box_label"].strip()

    # 2. New set not in inventory: check callback first
    if prompt_callback:
        user_box = prompt_callback(set_code)
        if user_box and user_box.strip():
            return user_box.strip()

    # 3. Prompt user via stdin if interactive
    if interactive and sys.stdin.isatty():
        print(f"\n[NEW SET DETECTED] Set '{set_code}' does not currently exist in inventory.")
        user_box = input(f"Enter physical box label for '{set_code}' (e.g. M1..M19): ").strip()
        while not user_box:
            user_box = input(f"Box label cannot be empty. Enter box label for '{set_code}': ").strip()
        return user_box

    raise ValueError(f"Set '{set_code}' does not exist in inventory and no box was provided.")


def ingest_csv_batch(
    file_path: str,
    box_label: Optional[str] = None,
    section_label: Optional[str] = None,
    output_dir: str = "exports",
    push_to_api: bool = False,
    interactive: bool = True,
    prompt_callback: Optional[Any] = None,
) -> Dict[str, Any]:
    """Ingests cards from a ManaPool CSV, auto-resolving physical boxes and sections.

    Auto-resolution workflow:
    1. Keeps in-stock singles; other product types are skipped and counted.
    2. Resolves every row against the local Scryfall mirror. If any row cannot be
       resolved, nothing is written and the unresolved rows are returned (refresh the
       mirror or fix the CSV, then re-run).
    3. Tokens (by Scryfall layout) from every set form one new TOKEN-n chunk in the token box.
    4. Every other set resolves its box from inventory (or prompts the operator for a new
       set) and gets its next section (Sec-1 -> Sec-2 -> Sec-3).
    5. Sorts each new section in shelf order and assigns index_num = 1..N.
    6. Commits to the PostgreSQL inventory table and writes ManaPool upload CSVs.

    Args:
        file_path (str): Input ManaPool CSV file path.
        box_label (Optional[str]): Optional box override for regular sets (tokens always
            go to the token box).
        section_label (Optional[str]): Optional section override for regular sets.
        output_dir (str): Directory for export files.
        push_to_api (bool): Whether to push directly to ManaPool API.
        interactive (bool): Whether to prompt the user if an unknown set is encountered.
        prompt_callback (Optional[Any]): Callback function to prompt user in custom UI.

    Returns:
        Dict[str, Any]: Summary dictionary of the ingestion operation. status is 'success',
            'empty' or 'unresolved' (with 'unresolved' rows listed).
    """
    os.makedirs(output_dir, exist_ok=True)
    active_rows = read_manapool_csv(file_path)
    raw_rows = [r for r in active_rows if (r.get("product_type") or "mtg_single").strip() == "mtg_single"]
    skipped_non_singles = len(active_rows) - len(raw_rows)
    if not raw_rows:
        return {"status": "empty", "message": "No singles with quantity > 0 found in CSV."}

    resolved = scryfall_mirror.resolve_manapool_cards((r.get("set") or "", r.get("number") or "") for r in raw_rows)

    def res_for(r: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return resolved.get(((r.get("set") or "").strip().upper(), str(r.get("number") or "").strip().lower()))

    unresolved = [r for r in raw_rows if res_for(r) is None]
    if unresolved:
        return {
            "status": "unresolved",
            "message": (
                f"{len(unresolved)} rows are not in the Scryfall mirror; nothing was ingested. "
                "Refresh the mirror (python -m station.scryfall_mirror refresh) or fix the CSV, then re-run."
            ),
            "unresolved": unresolved,
        }

    # Group by shelf set: every token goes to 'TOKENS', everything else to its own set.
    groups: Dict[str, List[Dict[str, Any]]] = {}
    for r in raw_rows:
        shelf_set = db.TOKENS_SET if res_for(r)["is_token"] else (r.get("set") or "").strip().upper()
        groups.setdefault(shelf_set, []).append(r)

    all_inventory_to_insert: List[Dict[str, Any]] = []
    generated_csvs: List[str] = []
    set_boxes: Dict[str, str] = {}
    set_sections: Dict[str, str] = {}
    api_update_count = 0
    api_errors: List[str] = []

    for shelf_set in sorted(groups):
        rows = groups[shelf_set]
        is_tokens = shelf_set == db.TOKENS_SET
        target_box = (None if is_tokens else box_label) or resolve_set_box(
            shelf_set,
            interactive=interactive,
            prompt_callback=prompt_callback,
        )
        actual_section = (None if is_tokens else section_label) or db.get_next_section(target_box, shelf_set)
        set_boxes[shelf_set] = target_box
        set_sections[shelf_set] = actual_section

        sort_key = import_live_inventory._token_sort_key if is_tokens else import_live_inventory._set_sort_key
        for idx, r in enumerate(sorted(rows, key=sort_key), start=1):
            res = res_for(r)
            price_str = r.get("price") or r.get("market_price")
            try:
                price = float(price_str) if price_str else None
            except ValueError:
                price = None
            all_inventory_to_insert.append({
                "scryfall_id": res["front"]["scryfall_id"],
                "back_scryfall_id": res["back"]["scryfall_id"] if res["back"] else None,
                "manapool_product_id": (r.get("product_id") or "").strip(),
                "condition": (r.get("condition") or "NM").strip(),
                "is_foil": import_live_inventory._is_foil(r),
                "quantity": int(r.get("quantity", 1)),
                "manapool_listed_price": price,
                "box_label": target_box,
                "section_label": actual_section,
                "index_num": idx,
            })

        csv_path = generate_manapool_upload_csv(set_code=shelf_set, rows=rows, output_dir=output_dir)
        generated_csvs.append(csv_path)

    db.insert_inventory_batch(all_inventory_to_insert)

    if push_to_api and manapool_client.is_configured():
        api_update_count, api_errors = manapool_client.push_inventory_batch(all_inventory_to_insert)

    total_cards = sum(i["quantity"] for i in all_inventory_to_insert)
    return {
        "status": "success",
        "box_label": box_label or ", ".join(sorted(set(set_boxes.values()))),
        "set_boxes": set_boxes,
        "set_sections": set_sections,
        "sets": sorted(groups),
        "total_lines": len(all_inventory_to_insert),
        "total_cards": total_cards,
        "skipped_non_singles": skipped_non_singles,
        "upload_csvs": generated_csvs,
        "api_updated": api_update_count,
        "api_errors": api_errors,
    }


def generate_manapool_upload_csv(
    set_code: str,
    rows: List[Dict[str, Any]],
    output_dir: str = "exports",
) -> str:
    """Generates a clean CSV containing only positive quantity rows for ManaPool web upload.

    Args:
        set_code (str): Set code.
        rows (List[Dict[str, Any]]): Original row dictionaries with quantity > 0.
        output_dir (str): Output folder.

    Returns:
        str: Absolute path to the generated CSV.
    """
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"manapool_upload_{set_code}_{timestamp}.csv"
    filepath = os.path.join(output_dir, filename)

    if not rows:
        return ""

    fieldnames = list(rows[0].keys())
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    return os.path.abspath(filepath)


def relocate_box(
    source_box: str,
    target_box: str,
    set_code: str,
    section_label: Optional[str] = None,
) -> Dict[str, Any]:
    """Relocates cards for a given shelf set/section from one box to another.

    Args:
        source_box (str): Current box label.
        target_box (str): Destination box label.
        set_code (str): Set code, or 'TOKENS' to move every token chunk together.
        section_label (Optional[str]): Specific section or None for all sections of that set.

    Returns:
        Dict[str, Any]: Summary of relocation result.
    """
    updated_rows = db.relocate_inventory(
        source_box=source_box,
        target_box=target_box,
        set_code=set_code,
        section_label=section_label,
    )

    if updated_rows == 0:
        return {"status": "not_found", "message": f"No cards found matching {source_box} / {set_code}."}

    return {
        "status": "success",
        "updated_rows": updated_rows,
        "source_box": source_box,
        "target_box": target_box,
        "set_code": set_code,
    }


def list_inventory_table(box_label: Optional[str] = None) -> str:
    """Formats the current physical storage breakdown as a clean terminal table.

    Args:
        box_label (Optional[str]): Optional box filter.

    Returns:
        str: Formatted ASCII table string.
    """
    summary = db.get_inventory_summary(box_label)
    if not summary:
        return "No cards currently cataloged in physical inventory."

    rows = []
    for s in summary:
        index_range = f"{s['min_index']} - {s['max_index']}" if s["min_index"] else "N/A"
        rows.append([
            s["box_label"],
            s["set_code"].upper(),
            s["section_label"],
            s["total_lines"],
            s["total_cards"],
            index_range,
        ])

    return tabulate(
        rows,
        headers=["Box Label", "Set Code", "Section", "Unique Titles", "Total Cards", "Index Range"],
        tablefmt="fancy_grid",
    )


def main():
    """CLI entry point for csv_pipeline."""
    parser = argparse.ArgumentParser(description="MTG Interim CSV Ingestion & Inventory Pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Subcommand: ingest
    p_ingest = subparsers.add_parser("ingest", help="Ingest ManaPool CSV batch into physical inventory")
    p_ingest.add_argument("csv_file", help="Path to ManaPool set CSV file")
    p_ingest.add_argument("--box", default=None, help="Optional target box override (auto-resolved per set by default)")
    p_ingest.add_argument("--section", default=None, help="Optional section override (defaults to auto-allocated Sec-X)")
    p_ingest.add_argument("--output-dir", default="exports", help="Directory for generated export files")
    p_ingest.add_argument("--push-api", action="store_true", help="Push inventory directly to ManaPool API if configured")

    # Subcommand: relocate
    p_reloc = subparsers.add_parser("relocate", help="Relocate set or section between physical boxes (Box Shuffle)")
    p_reloc.add_argument("--source", required=True, help="Source box label (e.g. Box-1)")
    p_reloc.add_argument("--target", required=True, help="Target box label (e.g. Box-2)")
    p_reloc.add_argument("--set", required=True, help="Set code (e.g. ZNR), or TOKENS for all token chunks")
    p_reloc.add_argument("--section", default=None, help="Optional specific section (e.g. Sec-1)")

    # Subcommand: list
    p_list = subparsers.add_parser("list", help="List physical inventory storage sections")
    p_list.add_argument("--box", default=None, help="Optional box label filter")

    args = parser.parse_args()

    if args.command == "ingest":
        if args.box:
            print(f"Ingesting {args.csv_file} into override box {args.box}...")
        else:
            print(f"Ingesting {args.csv_file} (auto-resolving target box per set)...")
        res = ingest_csv_batch(
            file_path=args.csv_file,
            box_label=args.box,
            section_label=args.section,
            output_dir=args.output_dir,
            push_to_api=args.push_api,
        )
        if res.get("status") == "success":
            print(f"Successfully ingested {res['total_cards']} cards across {res['total_lines']} titles.")
            print("Set Allocations:")
            for s_code, b_lbl in res.get("set_boxes", {}).items():
                print(f"  - Set {s_code} -> Box {b_lbl} / {res['set_sections'][s_code]}")
            if res["skipped_non_singles"]:
                print(f"Skipped {res['skipped_non_singles']} non-single rows.")
            print("Generated ManaPool upload CSVs:")
            for c in res["upload_csvs"]:
                print(f"  - {c}")
            if args.push_api:
                print(f"ManaPool API Sync: Updated {res['api_updated']} items directly.")
                for err in res["api_errors"]:
                    print(f"  [API Warning] {err}", file=sys.stderr)
        else:
            print(f"Ingestion result: {res.get('message')}")
            for r in res.get("unresolved", []):
                print(f"  - {r.get('set')} {r.get('number')} {r.get('name')}")

    elif args.command == "relocate":
        print(f"Relocating [{args.set}] from {args.source} to {args.target}...")
        res = relocate_box(
            source_box=args.source,
            target_box=args.target,
            set_code=args.set,
            section_label=args.section,
        )
        if res.get("status") == "success":
            print(f"Relocated {res['updated_rows']} inventory items to {args.target}.")
        else:
            print(f"Relocation failed: {res.get('message')}")

    elif args.command == "list":
        print(list_inventory_table(args.box))


if __name__ == "__main__":
    main()
