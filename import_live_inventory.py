"""Import live ManaPool inventory backup into PostgreSQL using physical Set -> Box mapping.

Reads:
1. ManaPool live inventory export (e.g. backups/inventory-manapool-backup-20260920.csv)
2. Physical set-to-box map ('Manapool Print Format - Set_Box Map.csv')

Assigns every card to its actual physical storage box (M1..M19), alphabetizes
within each set, assigns index numbers (1..N), and populates PostgreSQL in seconds.
"""

import argparse
import csv
import os
import re
import sys
from typing import Any, Dict, List, Optional, Tuple

from tabulate import tabulate
import db


def load_set_box_map(map_path: str) -> Dict[str, str]:
    """Parses 'Manapool Print Format - Set_Box Map.csv' to extract set_code -> box mapping.

    Format expected:
        set_label,Box
        (10E) Tenth Edition,M1
        (2X2) Double Masters 2022,M10

    Args:
        map_path (str): Path to Set_Box map CSV.

    Returns:
        Dict[str, str]: Dictionary mapping uppercase set code to physical box label (e.g. '10E' -> 'M1').
    """
    if not os.path.isfile(map_path):
        raise FileNotFoundError(f"Set-to-box map not found at '{map_path}'")

    set_to_box: Dict[str, str] = {}
    with open(map_path, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            label = row.get("set_label", "")
            box = (row.get("Box") or "").strip()
            if not box:
                continue
            m = re.search(r"\(([A-Za-z0-9]+)\)", label)
            if m:
                set_code = m.group(1).upper()
                set_to_box[set_code] = box

    return set_to_box


def import_live_inventory(
    inventory_csv: str,
    map_csv: str = "Manapool Print Format - Set_Box Map.csv",
    unmapped_box: str = "Unassigned",
    dry_run: bool = False,
    clear_existing: bool = False,
) -> Dict[str, Any]:
    """Imports live ManaPool backup into PostgreSQL mapped to real physical boxes.

    Args:
        inventory_csv (str): Path to exported ManaPool inventory CSV.
        map_csv (str): Path to Set_Box map CSV.
        unmapped_box (str): Default box for sets without a mapped location.
        dry_run (bool): If True, parses and tabulates statistics without modifying database.
        clear_existing (bool): If True, truncates existing inventory table before inserting.

    Returns:
        Dict[str, Any]: Execution summary dictionary.
    """
    if not os.path.isfile(inventory_csv):
        raise FileNotFoundError(f"Inventory CSV not found at '{inventory_csv}'")

    print(f"Loading physical Set -> Box mappings from '{map_csv}'...")
    set_to_box = load_set_box_map(map_csv)
    print(f"Loaded {len(set_to_box)} set-to-box definitions across {len(set(set_to_box.values()))} physical boxes.")

    print(f"Reading live inventory backup from '{inventory_csv}'...")
    with open(inventory_csv, mode="r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        raw_rows = list(reader)

    print(f"Read {len(raw_rows):,} total inventory lines. Filtering active stock...")
    active_rows = [r for r in raw_rows if int(r.get("quantity", 0) or 0) > 0]
    total_physical_cards = sum(int(r.get("quantity", 1)) for r in active_rows)
    print(f"Active in-stock listings: {len(active_rows):,} ({total_physical_cards:,} physical cards).")

    all_cards_to_upsert: Dict[str, Dict[str, Any]] = {}
    all_inventory_to_insert: List[Dict[str, Any]] = []
    box_stats: Dict[str, Dict[str, Any]] = {}
    unmapped_sets: List[str] = []

    def record_card_and_inv(
        it: Dict[str, Any],
        box_label: str,
        idx: int,
        section_label: str = "Sec-1",
    ) -> None:
        set_code = (it.get("set") or "").strip().upper()
        num = str(it.get("number") or "").strip()
        name = (it.get("name") or "").strip()
        rarity = (it.get("rarity") or "unknown").strip().lower()
        prod_id = (it.get("product_id") or "").strip()
        finish = (it.get("finish") or "NF").strip().upper()
        is_foil = (finish in ["FO", "FOIL", "F"])
        cond = (it.get("condition") or "NM").strip()

        price_str = it.get("price") or it.get("market_price")
        try:
            price = float(price_str) if price_str else None
        except ValueError:
            price = None

        card_key = f"{set_code}:{num}".lower()
        synth_id = f"mp-{set_code.lower()}-{num.lower()}"

        if card_key not in all_cards_to_upsert:
            all_cards_to_upsert[card_key] = {
                "scryfall_id": synth_id,
                "name": name,
                "set_code": set_code,
                "collector_number": num,
                "rarity": rarity,
                "image_uri": None,
                "image_hash": None,
            }

        all_inventory_to_insert.append({
            "scryfall_id": synth_id,
            "manapool_product_id": prod_id,
            "condition": cond,
            "is_foil": is_foil,
            "quantity": int(it.get("quantity", 1)),
            "manapool_listed_price": price,
            "box_label": box_label,
            "section_label": section_label,
            "index_num": idx,
        })

    # Separate tokens / emblems, standalone promos, and regular expansion cards
    tokens_items: List[Dict[str, Any]] = []
    promo_items: List[Dict[str, Any]] = []
    regular_sets: Dict[str, List[Dict[str, Any]]] = {}

    promo_sets = {"PECL", "PKHM", "PSOS"}

    for r in active_rows:
        s = (r.get("set") or "").strip().upper()
        name_lower = (r.get("name") or "").strip().lower()
        is_token = (
            (s.startswith("T") and len(s) >= 4)
            or (s == "DDR" and "emblem" in name_lower)
        )
        if is_token:
            tokens_items.append(r)
        elif s in promo_sets:
            promo_items.append(r)
        else:
            if s:
                regular_sets.setdefault(s, []).append(r)

    # 1. Process Dedicated TOKENS Section in Box M19: globally alphabetized matching divider_TOKENS.txt
    if tokens_items:
        def token_sort_key(x: Dict[str, Any]):
            num_digits = "".join(c for c in str(x.get("number") or "") if c.isdigit())
            num_val = int(num_digits) if num_digits else 0
            return (
                (x.get("name") or "").strip().lower(),
                (x.get("set") or "").strip().upper(),
                num_val,
                (x.get("finish") or "").strip().upper()
            )

        tokens_items.sort(key=token_sort_key)
        m19_stats = box_stats.setdefault("M19", {"sets": set(), "listings": 0, "cards": 0})
        m19_stats["sets"].update(t["set"].upper() for t in tokens_items)
        m19_stats["listings"] += len(tokens_items)
        m19_stats["cards"] += sum(int(t.get("quantity", 1)) for t in tokens_items)

        for idx, it in enumerate(tokens_items, start=1):
            record_card_and_inv(it, "M19", idx, section_label="TOKENS")

    # 2. Process Dedicated PROMOS Section in Box M19
    if promo_items:
        promo_items.sort(key=lambda x: (
            (x.get("name") or "").strip().lower(),
            (x.get("set") or "").strip().upper(),
            str(x.get("number") or "").strip().lower(),
        ))
        m19_stats = box_stats.setdefault("M19", {"sets": set(), "listings": 0, "cards": 0})
        m19_stats["sets"].update(p["set"].upper() for p in promo_items)
        m19_stats["listings"] += len(promo_items)
        m19_stats["cards"] += sum(int(p.get("quantity", 1)) for p in promo_items)

        for idx, it in enumerate(promo_items, start=1):
            record_card_and_inv(it, "M19", idx, section_label="PROMOS")

    # 3. Process Regular Expansion Sets (M1..M19, etc.)
    for set_code, items in sorted(regular_sets.items()):
        box_label = set_to_box.get(set_code)
        if not box_label:
            box_label = unmapped_box
            if set_code not in unmapped_sets:
                unmapped_sets.append(set_code)

        if box_label not in box_stats:
            box_stats[box_label] = {"sets": set(), "listings": 0, "cards": 0}
        box_stats[box_label]["sets"].add(set_code)
        box_stats[box_label]["listings"] += len(items)
        box_stats[box_label]["cards"] += sum(int(it.get("quantity", 1)) for it in items)

        items.sort(key=lambda x: (
            (x.get("name") or "").strip().lower(),
            str(x.get("number") or "").strip().lower(),
            (x.get("finish") or "").strip().upper()
        ))

        for idx, it in enumerate(items, start=1):
            record_card_and_inv(it, box_label, idx, section_label="Sec-1")

    if not dry_run:
        if clear_existing:
            print("\nTruncating existing PostgreSQL 'inventory' table...")
            with db.get_db_cursor() as cur:
                cur.execute("TRUNCATE TABLE inventory RESTART IDENTITY;")

        print(f"\nUpserting {len(all_cards_to_upsert):,} unique titles into PostgreSQL 'cards' table...")
        db.upsert_cards_batch(list(all_cards_to_upsert.values()))

        print(f"Inserting {len(all_inventory_to_insert):,} physical inventory records into PostgreSQL 'inventory' table...")
        db.insert_inventory_batch(all_inventory_to_insert)
        print("Database commit successful!")

    # Format summary table
    summary_rows = []
    for b_label in sorted(box_stats.keys()):
        b_info = box_stats[b_label]
        sample_sets = ", ".join(sorted(list(b_info["sets"]))[:4])
        if len(b_info["sets"]) > 4:
            sample_sets += f" (+{len(b_info['sets']) - 4} more)"

        summary_rows.append([
            b_label,
            len(b_info["sets"]),
            sample_sets,
            f"{b_info['listings']:,}",
            f"{b_info['cards']:,}",
        ])

    table_output = tabulate(
        summary_rows,
        headers=["Physical Box", "Sets Count", "Sample Sets", "Listings", "Total Cards"],
        tablefmt="fancy_grid",
    )

    return {
        "status": "success",
        "dry_run": dry_run,
        "total_lines": len(all_inventory_to_insert),
        "total_cards": total_physical_cards,
        "unique_cards": len(all_cards_to_upsert),
        "total_boxes": len(box_stats),
        "unmapped_sets_count": len(unmapped_sets),
        "unmapped_sets": unmapped_sets,
        "summary_table": table_output,
    }


def main():
    """CLI entry point for import_live_inventory."""
    parser = argparse.ArgumentParser(description="Seed PostgreSQL with live ManaPool inventory mapped to physical boxes")
    parser.add_argument(
        "--inventory-csv",
        default="backups/inventory-manapool-audited-20260927.csv",
        help="Path to ManaPool inventory export CSV (default: backups/inventory-manapool-audited-20260927.csv)",
    )
    parser.add_argument(
        "--map-csv",
        default="Manapool Print Format - Set_Box Map.csv",
        help="Path to Set_Box map CSV (default: Manapool Print Format - Set_Box Map.csv)",
    )
    parser.add_argument(
        "--unmapped-box",
        default="Unassigned",
        help="Box label for sets not found in map (default: Unassigned)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Parse and calculate mapping without modifying database",
    )
    parser.add_argument(
        "--clear-existing",
        action="store_true",
        help="Truncate PostgreSQL inventory table before inserting",
    )

    args = parser.parse_args()

    try:
        res = import_live_inventory(
            inventory_csv=args.inventory_csv,
            map_csv=args.map_csv,
            unmapped_box=args.unmapped_box,
            dry_run=args.dry_run,
            clear_existing=args.clear_existing,
        )

        print("\n" + res["summary_table"])
        mode_str = "[DRY RUN PREVIEW - NO DB CHANGES]" if res["dry_run"] else "[COMMITTED TO POSTGRESQL]"
        print(f"\n{mode_str}")
        print(f"Total Physical Cards: {res['total_cards']:,} across {res['total_lines']:,} listings.")
        print(f"Unique Card Titles:   {res['unique_cards']:,}")
        print(f"Physical Boxes Used: {res['total_boxes']}")

        if res["unmapped_sets_count"] > 0:
            print(f"\nNote: {res['unmapped_sets_count']} sets were not found in Set_Box map and placed into '{args.unmapped_box}':")
            print("  " + ", ".join(res["unmapped_sets"]))

    except Exception as e:
        print(f"Error during import: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
