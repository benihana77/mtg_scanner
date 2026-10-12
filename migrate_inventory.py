"""Migration and baseline seeding module for existing ManaPool inventory.

Allows seeding the local PostgreSQL database with existing storefront stock,
either by downloading live listings via the ManaPool API (using cursor pagination)
or by ingesting an exported inventory CSV backup.
"""

import argparse
import os
import sys
import time
from typing import Any, Dict, List, Optional

import requests

import db
import manapool_client
import csv_pipeline


def sync_inventory_from_api(box_label: str = "Box-0") -> int:
    """Downloads all active inventory from ManaPool API using cursor pagination

    and seeds it into the local database under box_label.

    Args:
        box_label (str): Box label to assign to existing stock (default 'Box-0').

    Returns:
        int: Total number of inventory items migrated.
    """
    if not manapool_client.is_configured():
        print("Error: ManaPool API credentials are not configured in .env", file=sys.stderr)
        return 0

    headers = manapool_client.get_headers()
    url = f"{manapool_client.BASE_URL}/seller/inventory"
    cursor = None
    total_migrated = 0
    page_num = 1

    print(f"Connecting to ManaPool API to retrieve storefront inventory...")

    while True:
        params: Dict[str, Any] = {"limit": 200, "minQuantity": 1}
        if cursor:
            params["cursor"] = cursor

        try:
            resp = requests.get(url, headers=headers, params=params, timeout=20)
            if resp.status_code != 200:
                print(f"Failed to fetch page {page_num}: HTTP {resp.status_code} - {resp.text[:200]}", file=sys.stderr)
                break

            data = resp.json()
            items = data.get("inventory", [])
            if not items:
                break

            cards_to_upsert = []
            inv_to_insert = []

            for idx, it in enumerate(items, start=1):
                if it.get("product_type") != "mtg_single":
                    continue

                prod = it.get("product", {})
                single = prod.get("single", {})
                if not single:
                    continue

                scryfall_id = single.get("scryfall_id")
                name = single.get("name", "Unknown Card")
                set_code = (single.get("set") or "").upper()
                collector_num = str(single.get("number") or "")
                cond = single.get("condition_id", "NM")
                finish = single.get("finish_id", "NF")
                is_foil = (finish in ["FO", "FOIL", "F"])
                qty = int(it.get("quantity", 1))
                price_cents = it.get("price_cents")
                price = (price_cents / 100.0) if price_cents else None
                manapool_prod_id = it.get("product_id")

                if scryfall_id:
                    cards_to_upsert.append({
                        "scryfall_id": scryfall_id,
                        "name": name,
                        "set_code": set_code,
                        "collector_number": collector_num,
                        "rarity": "unknown",
                        "image_uri": None,
                        "image_hash": None,
                    })

                    inv_to_insert.append({
                        "scryfall_id": scryfall_id,
                        "manapool_product_id": manapool_prod_id,
                        "condition": cond,
                        "is_foil": is_foil,
                        "quantity": qty,
                        "manapool_listed_price": price,
                        "box_label": box_label,
                        "section_label": "Sec-Legacy",
                        "index_num": idx,
                    })

            if cards_to_upsert:
                db.upsert_cards_batch(cards_to_upsert)
            if inv_to_insert:
                db.insert_inventory_batch(inv_to_insert)

            total_migrated += len(inv_to_insert)
            print(f"Page {page_num}: Migrated {len(inv_to_insert)} items (Total so far: {total_migrated})")

            # Check next cursor
            pagination = data.get("pagination", {})
            cursor = pagination.get("next_cursor")
            if not cursor:
                break

            page_num += 1
            time.sleep(0.05)

        except Exception as e:
            print(f"Exception during inventory migration: {e}", file=sys.stderr)
            break

    print(f"\nMigration finished: Successfully loaded {total_migrated} items into '{box_label}'.")
    return total_migrated


def main():
    """CLI interface for migrating or seeding inventory."""
    parser = argparse.ArgumentParser(description="Seed local PostgreSQL with ManaPool inventory")
    parser.add_argument("--csv", help="Path to exported ManaPool inventory CSV backup file")
    parser.add_argument("--api", action="store_true", help="Download live inventory directly via ManaPool API")
    parser.add_argument("--box", default="Box-0", help="Target box label for existing stock (default: Box-0)")

    args = parser.parse_args()

    if args.api:
        sync_inventory_from_api(box_label=args.box)
    elif args.csv:
        if not os.path.isfile(args.csv):
            print(f"Error: File '{args.csv}' not found.", file=sys.stderr)
            sys.exit(1)
        print(f"Ingesting CSV backup '{args.csv}' into {args.box}...")
        res = csv_pipeline.ingest_csv_batch(file_path=args.csv, box_label=args.box)
        print(f"Result: Ingested {res.get('total_cards', 0)} cards across {res.get('total_lines', 0)} titles.")
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
