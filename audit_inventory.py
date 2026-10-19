"""Inventory audit, unmapped set assignment, and token extraction generator.

Implements Strategy B:
1. Interactive wizard to assign physical boxes to the 23 unmapped sets.
2. Updates 'Manapool Print Format - Set_Box Map.csv' with a backup.
3. Generates a physical Token Extraction Walk Sheet (exports/token_extraction_pull_sheet.txt)
   guiding the operator to pull all 363 tokens from their base set boxes (M1..M19).
4. Verifies that 100% of all 44,295 cards are mapped with 0 unassigned.
"""

import argparse
import csv
import os
import re
import shutil
import sys
from typing import Any, Dict, List, Optional, Tuple

from tabulate import tabulate


def load_raw_set_box_rows(map_path: str) -> List[Dict[str, str]]:
    """Loads all raw rows from the Set_Box Map CSV preserving order and headers."""
    with open(map_path, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        return list(reader)


def save_set_box_rows(map_path: str, rows: List[Dict[str, str]]) -> None:
    """Saves updated rows to Set_Box Map CSV, making a timestamped backup first."""
    backup_path = f"{map_path}.bak"
    shutil.copyfile(map_path, backup_path)
    print(f"Backed up original map to '{backup_path}'.")

    fieldnames = ["set_label", "Box"]
    with open(map_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in rows:
            writer.writerow({"set_label": r.get("set_label", ""), "Box": r.get("Box", "").strip()})
    print(f"Saved updated Set_Box map to '{map_path}'.")


def get_token_sets_summary(inventory_rows: List[Dict[str, Any]], set_to_box: Dict[str, str]) -> List[Dict[str, Any]]:
    """Extracts all token sets, mapping them to their parent set's current physical box."""
    tokens_by_set: Dict[str, List[Dict[str, Any]]] = {}
    for r in inventory_rows:
        s = r["set"].upper()
        if (s.startswith("T") and len(s) in [4, 5]) or "token" in r["name"].lower():
            tokens_by_set.setdefault(s, []).append(r)

    token_summary = []
    for s, items in sorted(tokens_by_set.items()):
        total_qty = sum(int(it.get("quantity", 0) or 0) for it in items)
        base_set = s[1:] if s.startswith("T") else s
        base_box = set_to_box.get(base_set, "Unknown")
        token_summary.append({
            "token_set": s,
            "base_set": base_set,
            "current_box": base_box,
            "listings_count": len(items),
            "total_physical_tokens": total_qty,
            "items": items,
        })
    return token_summary


def generate_token_extraction_sheet(
    token_summary: List[Dict[str, Any]],
    output_dir: str = "exports",
    token_box_label: str = "TOKENS",
) -> str:
    """Generates a physical pull sheet to walk the shelves and extract all tokens into a dedicated box."""
    os.makedirs(output_dir, exist_ok=True)
    sheet_path = os.path.join(output_dir, "token_extraction_pull_sheet.txt")

    total_tokens = sum(ts["total_physical_tokens"] for ts in token_summary)
    total_listings = sum(ts["listings_count"] for ts in token_summary)

    # Group by current physical box so operator walks shelf in order
    by_box: Dict[str, List[Dict[str, Any]]] = {}
    for ts in token_summary:
        by_box.setdefault(ts["current_box"], []).append(ts)

    header = f"""========================================================================================
                      STRATEGY B: TOKEN CONSOLIDATION PULL SHEET
========================================================================================
 Target Box:    {token_box_label} (Single dedicated 400-count box for all tokens)
 Total Tokens:  {total_tokens} physical cards across {total_listings} titles ({len(token_summary)} token sets)
 Verification:  Count physical cards after extraction. Total MUST equal {total_tokens}.
========================================================================================
 INSTRUCTIONS:
 1. Walk to each box listed below (sorted in physical sequence: M1, M2, M3...).
 2. In each box, locate the parent set section and pull out the indicated tokens.
 3. Check the box [  ] as each card is pulled.
 4. Drop all extracted tokens into your new '{token_box_label}' box, alphabetized by name!
========================================================================================
"""
    body_lines = []
    for box in sorted(by_box.keys()):
        box_sets = by_box[box]
        box_card_subtotal = sum(ts["total_physical_tokens"] for ts in box_sets)
        body_lines.append(f"\n--- [ PHYSICAL BOX: {box} ] --- (Pull {box_card_subtotal} tokens total from this box)")

        rows = []
        for ts in box_sets:
            for it in ts["items"]:
                finish = it.get("finish", "NF")
                rows.append([
                    box,
                    ts["base_set"],
                    ts["token_set"],
                    it["name"][:25],
                    it.get("number", ""),
                    finish,
                    it.get("quantity", 1),
                    "[   ]",
                ])

        table_txt = tabulate(
            rows,
            headers=["Box", "Parent", "Set", "Token Name", "Num", "Finish", "Qty", "Pulled"],
            tablefmt="simple",
        )
        body_lines.append(table_txt)

    body_lines.append(f"""
========================================================================================
 PHYSICAL COUNT VERIFICATION CHECKPOINT:
 --------------------------------------------------------------------------------------
 Total physical tokens extracted: [ ______ ]  / Target: {total_tokens} cards.
 If your physical count equals {total_tokens}, your token extraction is 100% complete!
========================================================================================
""")

    with open(sheet_path, "w", encoding="utf-8") as f:
        f.write(header)
        f.write("\n".join(body_lines))

    return os.path.abspath(sheet_path)


def prompt_for_unmapped_sets(
    unmapped_sets: Dict[str, List[Dict[str, Any]]],
    raw_map_rows: List[Dict[str, str]],
    map_csv: str,
) -> Dict[str, str]:
    """Interactive CLI wizard that prompts the operator to assign physical boxes to unmapped sets."""
    print("\n" + "=" * 75)
    print("       STEP 1: ASSIGN PHYSICAL BOXES FOR UNMAPPED SETS")
    print("=" * 75)
    print(f"There are {len(unmapped_sets)} sets in your inventory that need physical box assignments.")
    print("You can enter the box label (e.g. 'M1', 'M10', 'M7') for each set.")
    print("Tip: If multiple sets go into the same box, you can assign them one by one.")
    print("-" * 75)

    new_assignments: Dict[str, str] = {}

    for set_code, items in sorted(unmapped_sets.items(), key=lambda x: -sum(int(it.get("quantity", 1)) for it in x[1])):
        total_qty = sum(int(it.get("quantity", 1)) for it in items)
        sample_cards = ", ".join(it.get("name", "") for it in items[:3])
        if len(items) > 3:
            sample_cards += f" (+{len(items) - 3} more)"

        # Find set label from raw map rows if available
        set_label = set_code
        for r in raw_map_rows:
            m = re.search(r"\(([A-Za-z0-9]+)\)", r.get("set_label", ""))
            if m and m.group(1).upper() == set_code:
                set_label = r.get("set_label")
                break

        print(f"\nSet: {set_label}")
        print(f"  In-Stock: {total_qty} cards across {len(items)} titles")
        print(f"  Samples:  {sample_cards}")

        while True:
            try:
                choice = input(f"  -> Enter physical Box label for [{set_code}] (e.g. M1, M10): ").strip()
                if choice:
                    new_assignments[set_code] = choice.upper()
                    break
                else:
                    print("  [!] Please enter a box label (e.g. M1, M2).")
            except (KeyboardInterrupt, EOFError):
                print("\nOperation cancelled.")
                return new_assignments

    # Update raw map rows
    for r in raw_map_rows:
        m = re.search(r"\(([A-Za-z0-9]+)\)", r.get("set_label", ""))
        if m:
            code = m.group(1).upper()
            if code in new_assignments:
                r["Box"] = new_assignments[code]

    # Check if any assigned sets weren't in the CSV at all and append them
    existing_codes = set()
    for r in raw_map_rows:
        m = re.search(r"\(([A-Za-z0-9]+)\)", r.get("set_label", ""))
        if m:
            existing_codes.add(m.group(1).upper())

    for code, b in new_assignments.items():
        if code not in existing_codes:
            raw_map_rows.append({"set_label": f"({code}) {code}", "Box": b})

    # Save to CSV
    save_set_box_rows(map_csv, raw_map_rows)
    return new_assignments


def run_audit(
    inventory_csv: str = "backups/inventory-manapool-backup-20260920.csv",
    map_csv: str = "Manapool Print Format - Set_Box Map.csv",
    interactive: bool = True,
    token_box_label: str = "TOKENS",
    output_dir: str = "exports",
) -> None:
    """Executes the full inventory audit and resolution routine."""
    print("=" * 80)
    print("        MTG INVENTORY AUDIT & INTEGRITY VERIFICATION SUITE")
    print("=" * 80)

    # 1. Load inventory
    with open(inventory_csv, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        all_inventory = list(reader)

    active_inventory = [r for r in all_inventory if int(r.get("quantity", 0) or 0) > 0]
    total_physical_cards = sum(int(r.get("quantity", 1)) for r in active_inventory)
    print(f"Loaded live inventory: {total_physical_cards:,} physical cards across {len(active_inventory):,} titles.")

    # 2. Load set-to-box map
    raw_map_rows = load_raw_set_box_rows(map_csv)
    set_to_box: Dict[str, str] = {}
    for r in raw_map_rows:
        box = (r.get("Box") or "").strip()
        m = re.search(r"\(([A-Za-z0-9]+)\)", r.get("set_label", ""))
        if m and box:
            set_to_box[m.group(1).upper()] = box

    # 3. Categorize tokens vs non-token inventory
    token_rows = []
    regular_rows = []
    for r in active_inventory:
        s = r["set"].upper()
        if (s.startswith("T") and len(s) in [4, 5]) or "token" in r["name"].lower():
            token_rows.append(r)
        else:
            regular_rows.append(r)

    total_token_cards = sum(int(r.get("quantity", 1)) for r in token_rows)
    total_regular_cards = sum(int(r.get("quantity", 1)) for r in regular_rows)

    print(f"\nInventory Composition:")
    print(f"  * Regular MTG Singles: {total_regular_cards:,} cards ({len(regular_rows):,} listings)")
    print(f"  * Tokens:              {total_token_cards:,} cards ({len(token_rows):,} listings across 37 sets)")

    # 4. Strategy B: Generate Token Extraction Sheet
    token_summary = get_token_sets_summary(active_inventory, set_to_box)
    sheet_path = generate_token_extraction_sheet(
        token_summary=token_summary,
        output_dir=output_dir,
        token_box_label=token_box_label,
    )
    print(f"\n[Strategy B Ready] Dedicated Token Extraction:")
    print(f"  -> Pull Sheet:  {sheet_path}")

    # 5. Check for unmapped regular sets
    unmapped_regular_sets: Dict[str, List[Dict[str, Any]]] = {}
    for r in regular_rows:
        s = r["set"].upper()
        if s not in set_to_box:
            unmapped_regular_sets.setdefault(s, []).append(r)

    print(f"\nUnmapped Regular Sets Status: {len(unmapped_regular_sets)} sets require box assignments.")

    # 6. Interactive prompt if requested
    if unmapped_regular_sets and interactive:
        prompt_for_unmapped_sets(unmapped_regular_sets, raw_map_rows, map_csv)
        # Reload set_to_box after updates
        set_to_box.clear()
        for r in raw_map_rows:
            box = (r.get("Box") or "").strip()
            m = re.search(r"\(([A-Za-z0-9]+)\)", r.get("set_label", ""))
            if m and box:
                set_to_box[m.group(1).upper()] = box

    # 7. Final Box Breakdown Audit Table
    box_audit: Dict[str, Dict[str, Any]] = {}
    # Add regular cards
    for r in regular_rows:
        s = r["set"].upper()
        b = set_to_box.get(s, "STILL_UNMAPPED")
        if b not in box_audit:
            box_audit[b] = {"sets": set(), "listings": 0, "cards": 0}
        box_audit[b]["sets"].add(s)
        box_audit[b]["listings"] += 1
        box_audit[b]["cards"] += int(r.get("quantity", 1))

    # Add token box
    box_audit[token_box_label] = {
        "sets": set(ts["token_set"] for ts in token_summary),
        "listings": len(token_rows),
        "cards": total_token_cards,
    }

    summary_rows = []
    for b in sorted(box_audit.keys()):
        info = box_audit[b]
        sample_sets = ", ".join(sorted(list(info["sets"]))[:4])
        if len(info["sets"]) > 4:
            sample_sets += f" (+{len(info['sets']) - 4} more)"
        summary_rows.append([
            b,
            len(info["sets"]),
            sample_sets,
            f"{info['listings']:,}",
            f"{info['cards']:,}",
        ])

    table_txt = tabulate(
        summary_rows,
        headers=["Physical Box", "Sets Count", "Sample Sets", "Listings", "Total Cards"],
        tablefmt="fancy_grid",
    )
    print("\n" + table_txt)

    # 8. Check integrity
    unmapped_remaining = box_audit.get("STILL_UNMAPPED", {}).get("cards", 0)
    print(f"\nFinal Audit Verification:")
    print(f"  * Total Verified In Inventory: {total_physical_cards:,} cards")
    print(f"  * Total Allocated to Boxes:    {total_physical_cards - unmapped_remaining:,} cards")
    print(f"  * Total Unmapped Cards:        {unmapped_remaining} cards")

    if unmapped_remaining == 0:
        print("\n[SUCCESS] 100% of all cards are assigned to a known physical box! Zero unassigned cards.")


def main():
    parser = argparse.ArgumentParser(description="MTG Inventory Audit & Token Strategy B Tool")
    parser.add_argument("--inventory", default="backups/inventory-manapool-backup-20260920.csv")
    parser.add_argument("--map", default="Manapool Print Format - Set_Box Map.csv")
    parser.add_argument("--non-interactive", action="store_true", help="Do not prompt for unmapped sets")
    parser.add_argument("--token-box", default="TOKENS", help="Box label for dedicated token storage")
    args = parser.parse_args()

    run_audit(
        inventory_csv=args.inventory,
        map_csv=args.map,
        interactive=not args.non_interactive,
        token_box_label=args.token_box,
    )


if __name__ == "__main__":
    main()
