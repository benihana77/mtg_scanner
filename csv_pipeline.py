"""ManaPool CSV ingestion, indexing, and physical storage pipeline.

Handles:
- Reading ManaPool set inventory spreadsheets.
- Filtering active batches (quantity > 0).
- Resolving card metadata from PostgreSQL catalog with Scryfall fallback.
- Auto-allocating physical section identifiers (Sec-1, Sec-2, etc.).
- Strict alphabetical sorting by card name within sections.
- Sequential index number assignment (1, 2, 3... N).
- Committing to PostgreSQL cards & inventory tables.
- Generating physical divider slips and ManaPool upload CSVs.
- Relocating sets/sections across boxes when capacity runs out.
"""

import argparse
import csv
import datetime
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from tabulate import tabulate

import db
import manapool_client

SCRYFALL_API_URL = "https://api.scryfall.com/cards"
SCRYFALL_HEADERS = {"User-Agent": os.getenv("SCRYFALL_USER_AGENT", "mtg_scanner/1.0")}


def fetch_scryfall_card(set_code: str, collector_number: str) -> Optional[Dict[str, Any]]:
    """Fetches card metadata directly from Scryfall API by set and collector number.

    Args:
        set_code (str): MTG set code (e.g. 'ZNR').
        collector_number (str): Card collector number (e.g. '89' or '7d').

    Returns:
        Optional[Dict[str, Any]]: Normalized card metadata dictionary if found.
    """
    url = f"{SCRYFALL_API_URL}/{set_code.lower()}/{collector_number.lower()}"
    try:
        time.sleep(0.08)  # Comply with Scryfall rate limits (~10 requests/sec)
        resp = requests.get(url, headers=SCRYFALL_HEADERS, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            image_uri = None
            if "image_uris" in data:
                image_uri = data["image_uris"].get("normal")
            elif "card_faces" in data and len(data["card_faces"]) > 0:
                image_uri = data["card_faces"][0].get("image_uris", {}).get("normal")

            return {
                "scryfall_id": data["id"],
                "name": data["name"],
                "set_code": data["set"].upper(),
                "collector_number": str(data["collector_number"]),
                "rarity": data.get("rarity", "common"),
                "image_uri": image_uri,
                "image_hash": None,
            }
        else:
            return None
    except Exception as e:
        print(f"Scryfall query error for {set_code}/{collector_number}: {e}", file=sys.stderr)
        return None


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
    """Resolves the physical storage box for an MTG set from PostgreSQL inventory.

    1. Checks if the set already exists in PostgreSQL inventory.
    2. If not found in DB, the set does not currently exist in inventory:
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
    query = """
        SELECT DISTINCT i.box_label
        FROM inventory i
        JOIN cards c ON i.scryfall_id = c.scryfall_id
        WHERE UPPER(c.set_code) = %s
        LIMIT 1;
    """
    try:
        with db.get_db_cursor(commit=False) as cur:
            cur.execute(query, (set_code,))
            row = cur.fetchone()
            if row and row.get("box_label"):
                return row["box_label"].strip()
    except Exception:
        pass

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
    1. Groups input CSV by MTG set code.
    2. For each set:
       a. Automatically resolves existing box location from PostgreSQL inventory.
       b. If set is brand new, prompts the operator for a box label.
       c. Automatically increments to the next section (Sec-1 -> Sec-2 -> Sec-3).
    3. Alphabetizes cards within each new section and assigns index_num = 1..N.
    4. Commits to PostgreSQL cards & inventory tables.
    5. Generates physical divider slips (divider_<box>_<set>_<sec>.txt) and ManaPool upload CSVs.

    Args:
        file_path (str): Input ManaPool CSV file path.
        box_label (Optional[str]): Optional box override (if omitted, auto-resolved per set).
        section_label (Optional[str]): Optional section override (if omitted, auto-incremented).
        output_dir (str): Directory for export files.
        push_to_api (bool): Whether to push directly to ManaPool API.
        interactive (bool): Whether to prompt the user if an unknown set is encountered.
        prompt_callback (Optional[Any]): Callback function to prompt user in custom UI.

    Returns:
        Dict[str, Any]: Summary dictionary of the ingestion operation.
    """
    os.makedirs(output_dir, exist_ok=True)
    raw_rows = read_manapool_csv(file_path)
    if not raw_rows:
        return {"status": "empty", "message": "No cards with quantity > 0 found in CSV."}

    # Identify sets in this batch
    sets_in_batch = sorted(list({r.get("set", "").upper() for r in raw_rows if r.get("set")}))

    all_cards_to_upsert: List[Dict[str, Any]] = []
    all_inventory_to_insert: List[Dict[str, Any]] = []
    generated_slips: List[str] = []
    generated_csvs: List[str] = []
    set_boxes: Dict[str, str] = {}
    api_update_count = 0
    api_errors: List[str] = []

    for set_code in sets_in_batch:
        set_rows = [r for r in raw_rows if r.get("set", "").upper() == set_code]
        if not set_rows:
            continue

        # Automatically resolve physical box for this set
        target_box = box_label or resolve_set_box(
            set_code,
            interactive=interactive,
            prompt_callback=prompt_callback,
        )
        set_boxes[set_code] = target_box

        # Automatically allocate next section (Sec-1 for new sets, Sec-2, Sec-3 for new chunks)
        actual_section = section_label or db.get_next_section(target_box, set_code)

        # 1. Resolve metadata for each card
        resolved_items = []
        for r in set_rows:
            num = str(r.get("number", "")).strip()
            name = r.get("name", "").strip()
            prod_id = r.get("product_id", "").strip()
            finish = r.get("finish", "NF").strip()
            is_foil = (finish.upper() == "FO")
            condition = r.get("condition", "NM").strip()
            qty = int(r.get("quantity", 1))
            price_str = r.get("price") or r.get("market_price")
            try:
                price = float(price_str) if price_str else None
            except ValueError:
                price = None

            # Check DB cache
            card_info = db.get_card_by_set_number(set_code, num)
            if not card_info:
                # Query Scryfall
                card_info = fetch_scryfall_card(set_code, num)
                if card_info:
                    all_cards_to_upsert.append(card_info)
                else:
                    # Synthetic record fallback
                    synth_id = f"custom-{set_code.lower()}-{num.lower()}"
                    card_info = {
                        "scryfall_id": synth_id,
                        "name": name,
                        "set_code": set_code,
                        "collector_number": num,
                        "rarity": "unknown",
                        "image_uri": None,
                        "image_hash": None,
                    }
                    all_cards_to_upsert.append(card_info)

            resolved_items.append({
                "card": card_info,
                "raw_row": r,
                "name": card_info["name"] if card_info else name,
                "set_code": set_code,
                "collector_number": num,
                "product_id": prod_id,
                "condition": condition,
                "is_foil": is_foil,
                "quantity": qty,
                "price": price,
            })

        # 2. STRICT ALPHABETICAL SORTING within this section by card name
        resolved_items.sort(key=lambda x: (x["name"].lower(), x["collector_number"]))

        # 3. Assign sequential index numbers 1..N
        section_inventory: List[Dict[str, Any]] = []
        for idx, item in enumerate(resolved_items, start=1):
            inv_entry = {
                "scryfall_id": item["card"]["scryfall_id"],
                "manapool_product_id": item["product_id"],
                "condition": item["condition"],
                "is_foil": item["is_foil"],
                "quantity": item["quantity"],
                "manapool_listed_price": item["price"],
                "box_label": target_box,
                "section_label": actual_section,
                "index_num": idx,
                "custom_external_id": f"{target_box}:{set_code}:{actual_section}:{idx:03d}",
            }
            section_inventory.append(inv_entry)
            all_inventory_to_insert.append(inv_entry)

        # 4. Generate physical divider slip text file
        slip_path = generate_divider_slip(
            box_label=target_box,
            set_code=set_code,
            section_label=actual_section,
            indexed_items=resolved_items,
            output_dir=output_dir,
        )
        generated_slips.append(slip_path)

        # 5. Generate ManaPool upload CSV
        csv_path = generate_manapool_upload_csv(
            set_code=set_code,
            rows=[it["raw_row"] for it in resolved_items],
            output_dir=output_dir,
        )
        generated_csvs.append(csv_path)

    # 6. Commit all to PostgreSQL
    if all_cards_to_upsert:
        db.upsert_cards_batch(all_cards_to_upsert)
    if all_inventory_to_insert:
        db.insert_inventory_batch(all_inventory_to_insert)

    # 7. Push to ManaPool API if enabled
    if push_to_api and manapool_client.is_configured():
        api_update_count, api_errors = manapool_client.push_inventory_batch(all_inventory_to_insert)

    total_cards = sum(i["quantity"] for i in all_inventory_to_insert)
    return {
        "status": "success",
        "box_label": box_label or ", ".join(sorted(set(set_boxes.values()))),
        "set_boxes": set_boxes,
        "sets": sets_in_batch,
        "total_lines": len(all_inventory_to_insert),
        "total_cards": total_cards,
        "slips": generated_slips,
        "upload_csvs": generated_csvs,
        "api_updated": api_update_count,
        "api_errors": api_errors,
    }


def generate_divider_slip(
    box_label: str,
    set_code: str,
    section_label: str,
    indexed_items: List[Dict[str, Any]],
    output_dir: str = "exports",
) -> str:
    """Creates a print-ready text file index to drop behind the physical section divider.

    Args:
        box_label (str): Box name.
        set_code (str): Set code.
        section_label (str): Section name.
        indexed_items (List[Dict[str, Any]]): List of sorted cards with assigned indices.
        output_dir (str): Output folder.

    Returns:
        str: Absolute path to the created divider slip file.
    """
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
    file_slug = f"divider_{box_label}_{set_code}_{section_label}".replace(" ", "_")
    slip_file = os.path.join(output_dir, f"{file_slug}.txt")

    total_cards = sum(it["quantity"] for it in indexed_items)
    header_box = f"""========================================================================
                      PHYSICAL SECTION DIVIDER SLIP
========================================================================
 Box:     {box_label:<20} Set:     {set_code.upper():<10} Section: {section_label}
 Date:    {timestamp:<20} Cards:   {total_cards} cards across {len(indexed_items)} titles
 Sort:    Alphabetical by Name (Index 1 to {len(indexed_items)})
========================================================================
"""
    table_rows = []
    for idx, it in enumerate(indexed_items, start=1):
        foil_str = "FOIL" if it["is_foil"] else "NF"
        price_str = f"${it['price']:.2f}" if it["price"] else "N/A"
        table_rows.append([
            f"#{idx:03d}",
            it["name"][:35],
            it["collector_number"],
            foil_str,
            it["condition"],
            it["quantity"],
            price_str,
        ])

    table_text = tabulate(
        table_rows,
        headers=["Index", "Card Name", "Num", "Finish", "Cond", "Qty", "Price"],
        tablefmt="simple",
    )

    with open(slip_file, "w", encoding="utf-8") as f:
        f.write(header_box)
        f.write(table_text)
        f.write("\n========================================================================\n")

    return os.path.abspath(slip_file)


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
    output_dir: str = "exports",
) -> Dict[str, Any]:
    """Relocates cards for a given set/section from one box to another and generates new divider slips.

    Args:
        source_box (str): Current box label.
        target_box (str): Destination box label.
        set_code (str): Set code.
        section_label (Optional[str]): Specific section or None for all sections of that set.
        output_dir (str): Folder for updated slips.

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

    # Fetch updated items to regenerate divider slip for the new box
    query = """
        SELECT i.index_num, c.name, c.collector_number, i.is_foil, i.condition, i.quantity,
               i.manapool_listed_price as price, i.section_label
        FROM inventory i
        JOIN cards c ON i.scryfall_id = c.scryfall_id
        WHERE i.box_label = %s AND LOWER(c.set_code) = LOWER(%s)
    """
    params = [target_box, set_code]
    if section_label:
        query += " AND i.section_label = %s"
        params.append(section_label)
    query += " ORDER BY i.section_label, i.index_num;"

    with db.get_db_cursor(commit=False) as cur:
        cur.execute(query, tuple(params))
        moved_cards = [dict(r) for r in cur.fetchall()]

    # Group by section to generate divider slips
    sections: Dict[str, List[Dict[str, Any]]] = {}
    for card in moved_cards:
        sec = card["section_label"]
        sections.setdefault(sec, []).append(card)

    new_slips = []
    for sec_name, items in sections.items():
        slip = generate_divider_slip(
            box_label=target_box,
            set_code=set_code,
            section_label=sec_name,
            indexed_items=items,
            output_dir=output_dir,
        )
        new_slips.append(slip)

    return {
        "status": "success",
        "updated_rows": updated_rows,
        "source_box": source_box,
        "target_box": target_box,
        "set_code": set_code,
        "new_slips": new_slips,
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
    p_reloc.add_argument("--set", required=True, help="Set code (e.g. ZNR)")
    p_reloc.add_argument("--section", default=None, help="Optional specific section (e.g. Sec-1)")
    p_reloc.add_argument("--output-dir", default="exports", help="Directory for updated divider slips")

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
                print(f"  - Set {s_code} -> Box {b_lbl}")
            print("Generated physical divider slips:")
            for s in res["slips"]:
                print(f"  - {s}")
            print("Generated ManaPool upload CSVs:")
            for c in res["upload_csvs"]:
                print(f"  - {c}")
            if args.push_api:
                print(f"ManaPool API Sync: Updated {res['api_updated']} items directly.")
                for err in res["api_errors"]:
                    print(f"  [API Warning] {err}", file=sys.stderr)
        else:
            print(f"Ingestion result: {res.get('message')}")

    elif args.command == "relocate":
        print(f"Relocating [{args.set}] from {args.source} to {args.target}...")
        res = relocate_box(
            source_box=args.source,
            target_box=args.target,
            set_code=args.set,
            section_label=args.section,
            output_dir=args.output_dir,
        )
        if res.get("status") == "success":
            print(f"Relocated {res['updated_rows']} inventory items to {args.target}.")
            print("New physical divider slips generated:")
            for s in res["new_slips"]:
                print(f"  - {s}")
        else:
            print(f"Relocation failed: {res.get('message')}")

    elif args.command == "list":
        print(list_inventory_table(args.box))


if __name__ == "__main__":
    main()
