"""Fresh import of a ManaPool inventory backup into PostgreSQL against the Scryfall mirror.

Reads:
1. ManaPool inventory export (--inventory-csv, required)
2. Physical set-to-box map ('Manapool Print Format - Set_Box Map.csv'), including a
   '(TOKENS) Tokens' row for the token box.

Every single is resolved against `scryfall_cards` (real Scryfall IDs; double-sided token
combos get a front `scryfall_id` and a `back_scryfall_id`). Rows that cannot be resolved
are refused and listed. Tokens (by Scryfall layout) go to TOKEN-1 in the token box,
standalone promos to PROMOS, and every other set to one alphabetized Sec-1 in its box.

With --rebuild, the legacy `cards` table, `inventory` and `price_history` are dropped and
recreated from schema.sql in a single transaction before inserting, so a failure leaves
the old database untouched. Without --rebuild nothing is written.

Sort keys follow the shelves: accent-insensitive names (Éomer under E), symbols first, and
same-name printings in numeric collector order (regular printing before showcase variants).
"""

import argparse
import csv
import datetime
import os
import re
import sys
import unicodedata
from typing import Any, Dict, List, Optional, Tuple

from psycopg2 import extras
from tabulate import tabulate

import db
import scryfall_mirror

TOKENS_SET_KEY = "TOKENS"
TOKEN_SECTION = "TOKEN-1"
PROMO_SETS = {"PECL", "PKHM", "PSOS"}

ProductKey = Tuple[str, bool]  # (manapool_product_id, is_foil)


def load_set_box_map(map_path: str) -> Dict[str, str]:
    """Parses 'Manapool Print Format - Set_Box Map.csv' to extract set_code -> box mapping.

    Format expected:
        set_label,Box
        (10E) Tenth Edition,M1
        (TOKENS) Tokens,M19

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


def _is_foil(row: Dict[str, Any]) -> bool:
    return (row.get("finish") or "NF").strip().upper() in ["FO", "FOIL", "F"]


def _product_key(row: Dict[str, Any]) -> ProductKey:
    return ((row.get("product_id") or "").strip(), _is_foil(row))


def name_sort_key(name: str) -> str:
    """Shelf alphabetization key: case- and accent-insensitive ('Éomer' files under E).

    Leading symbols are kept, so names like '+2 Mace' still file at the front of a section.
    """
    folded = unicodedata.normalize("NFKD", (name or "").strip().replace("Æ", "Ae").replace("æ", "ae"))
    return "".join(c for c in folded if not unicodedata.combining(c)).lower()


def collector_number_sort_key(number: str) -> Tuple[int, str]:
    """Numeric collector order, so the regular printing (#7) precedes showcase variants (#302).

    Numbers without leading digits (e.g. The List's 'CN2-126') sort as 0 then by text.
    """
    num = str(number or "").strip().lower()
    m = re.match(r"(\d+)", num)
    return (int(m.group(1)) if m else 0, num)


def _token_sort_key(x: Dict[str, Any]) -> Tuple[Any, ...]:
    """Token chunk order: ManaPool name -> set -> collector number -> finish."""
    return (
        name_sort_key(x.get("name") or ""),
        (x.get("set") or "").strip().upper(),
        collector_number_sort_key(x.get("number") or ""),
        (x.get("finish") or "").strip().upper(),
    )


def _promo_sort_key(x: Dict[str, Any]) -> Tuple[Any, ...]:
    return (
        name_sort_key(x.get("name") or ""),
        (x.get("set") or "").strip().upper(),
        collector_number_sort_key(x.get("number") or ""),
    )


def _set_sort_key(x: Dict[str, Any]) -> Tuple[Any, ...]:
    """Set section order: name -> collector number (regular printing first) -> finish."""
    return (
        name_sort_key(x.get("name") or ""),
        collector_number_sort_key(x.get("number") or ""),
        (x.get("finish") or "").strip().upper(),
    )


def fetch_current_layout() -> Dict[Tuple[str, str], List[ProductKey]]:
    """Reads the database's current physical order, before it is replaced.

    Returns:
        Dict[Tuple[str, str], List[ProductKey]]: (box, section) -> product keys in index order.
            The legacy 'TOKENS' section is reported as 'TOKEN-1' so the two compare directly.
    """
    layout: Dict[Tuple[str, str], List[ProductKey]] = {}
    with db.get_db_cursor(commit=False) as cur:
        cur.execute("SELECT to_regclass('public.inventory') IS NOT NULL AS present;")
        if not cur.fetchone()["present"]:
            return layout
        cur.execute("""
            SELECT manapool_product_id, is_foil, box_label, section_label, index_num
            FROM inventory WHERE quantity > 0
            ORDER BY box_label, section_label, index_num;
        """)
        for r in cur.fetchall():
            section = TOKEN_SECTION if r["section_label"] == "TOKENS" else r["section_label"]
            layout.setdefault((r["box_label"], section), []).append(
                ((r["manapool_product_id"] or "").strip(), bool(r["is_foil"]))
            )
    return layout


def build_resort_list(
    old_layout: Dict[Tuple[str, str], List[ProductKey]],
    new_layout: Dict[Tuple[str, str], List[ProductKey]],
    section_sets: Dict[Tuple[str, str], List[str]],
) -> List[Dict[str, Any]]:
    """Compares old and new physical order and lists sections that must be re-sorted.

    Old sections are split per set (a regular box holds many sets, each with its own Sec-n),
    so only sets whose card sequence actually changed are listed.

    Args:
        old_layout: (box, section) -> product keys in the current database order.
        new_layout: (box, section) -> product keys in the new import order.
        section_sets: (box, section) -> set code of each product in new_layout, same order.

    Returns:
        List[Dict[str, Any]]: One entry per (box, section, set) whose order changes.
    """
    product_set: Dict[ProductKey, str] = {}
    for loc, keys in new_layout.items():
        for key, set_code in zip(keys, section_sets[loc]):
            product_set[key] = set_code

    def split_by_set(keys: List[ProductKey], loc: Tuple[str, str]) -> Dict[str, List[ProductKey]]:
        # Token chunks hold many sets but are one physical run; keep them whole.
        if loc[1].upper().startswith("TOKEN"):
            return {TOKENS_SET_KEY: keys}
        out: Dict[str, List[ProductKey]] = {}
        for k in keys:
            out.setdefault(product_set.get(k, "?"), []).append(k)
        return out

    old_split = {(loc, s): ks for loc, keys in old_layout.items() for s, ks in split_by_set(keys, loc).items()}
    new_split = {(loc, s): ks for loc, keys in new_layout.items() for s, ks in split_by_set(keys, loc).items()}

    changes: List[Dict[str, Any]] = []
    for (loc, set_code) in sorted(set(old_split) | set(new_split)):
        old_keys, new_keys = old_split.get((loc, set_code), []), new_split.get((loc, set_code), [])
        if old_keys == new_keys:
            continue
        old_set, new_set = set(old_keys), set(new_keys)
        changes.append({
            "box_label": loc[0],
            "section_label": loc[1],
            "set_code": set_code,
            "cards_in": len(new_set - old_set),
            "cards_out": len(old_set - new_set),
            "reordered": old_set == new_set,
            "new_count": len(new_keys),
        })
    return changes


def rebuild_database(inventory_rows: List[Dict[str, Any]]) -> None:
    """Drops legacy catalog/inventory tables, recreates them and inserts rows, atomically.

    `scryfall_cards` is kept (CREATE IF NOT EXISTS). Any error rolls everything back.

    Args:
        inventory_rows (List[Dict[str, Any]]): Rows for the new inventory table.
    """
    base_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(base_dir, "schema.sql"), "r", encoding="utf-8") as f:
        schema_sql = f.read()

    query = """
        INSERT INTO inventory (
            scryfall_id, back_scryfall_id, manapool_product_id, condition, is_foil,
            quantity, manapool_listed_price, box_label, section_label, index_num
        ) VALUES %s;
    """
    params = [
        (
            r["scryfall_id"], r["back_scryfall_id"], r["manapool_product_id"], r["condition"],
            r["is_foil"], r["quantity"], r["manapool_listed_price"], r["box_label"],
            r["section_label"], r["index_num"],
        )
        for r in inventory_rows
    ]
    with db.get_db_cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS price_history, inventory, cards CASCADE;")
        cur.execute(schema_sql)
        extras.execute_values(cur, query, params, page_size=1000)


def write_summary_file(
    refused: List[Dict[str, Any]],
    resort: List[Dict[str, Any]],
    output_dir: str,
) -> str:
    """Writes the refused rows and the re-sort list to a printable text file.

    Returns:
        str: Absolute path to the summary file.
    """
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"import_summary_{datetime.date.today().isoformat()}.txt")
    with open(path, "w", encoding="utf-8") as f:
        f.write("SECTIONS WHOSE ORDER DIFFERS FROM THE CURRENT DATABASE\n")
        f.write("(The shelves follow the new order; these are database corrections unless a set was shelved by the old order.)\n")
        if resort:
            f.write(tabulate(
                [[c["box_label"], c["set_code"], c["section_label"], c["new_count"], c["cards_in"],
                  c["cards_out"], "yes" if c["reordered"] else "", "[   ]"] for c in resort],
                headers=["Box", "Set", "Section", "Cards", "In", "Out", "Reorder only", "Done"],
                tablefmt="simple",
            ) + "\n")
        else:
            f.write("  (none - order is unchanged)\n")
        f.write("\nREFUSED ROWS (not resolvable against scryfall_cards; not imported)\n")
        if refused:
            f.write(tabulate(
                [[r.get("set"), r.get("number"), (r.get("name") or "")[:40], r.get("finish"), r.get("quantity")]
                 for r in refused],
                headers=["Set", "Num", "Name", "Finish", "Qty"],
                tablefmt="simple",
            ) + "\n")
        else:
            f.write("  (none)\n")
    return os.path.abspath(path)


def plan_import(
    inventory_csv: str,
    map_csv: str = "Manapool Print Format - Set_Box Map.csv",
    unmapped_box: str = "Unassigned",
) -> Dict[str, Any]:
    """Resolves a ManaPool inventory backup and assigns every row its physical location.

    Reads only; nothing is written.

    Args:
        inventory_csv (str): Path to exported ManaPool inventory CSV.
        map_csv (str): Path to Set_Box map CSV.
        unmapped_box (str): Default box for sets (or TOKENS) without a mapped location.

    Returns:
        Dict[str, Any]: 'placements' (one dict per row: row, res, box_label, section_label,
            index_num), 'inventory_rows', 'new_layout', 'section_sets', 'box_stats',
            'refused', 'skipped_non_singles' and 'unmapped_sets'.

    Raises:
        RuntimeError: If the Scryfall mirror is empty.
    """
    if not os.path.isfile(inventory_csv):
        raise FileNotFoundError(f"Inventory CSV not found at '{inventory_csv}'")
    if scryfall_mirror.get_mirror_status()["card_count"] == 0:
        raise RuntimeError("scryfall_cards is empty; run 'python scryfall_mirror.py refresh' first.")

    set_to_box = load_set_box_map(map_csv)
    with open(inventory_csv, mode="r", encoding="utf-8-sig") as f:
        raw_rows = list(csv.DictReader(f))

    in_stock = [r for r in raw_rows if int(r.get("quantity", 0) or 0) > 0]
    singles = [r for r in in_stock if (r.get("product_type") or "").strip() == "mtg_single"]

    resolved = scryfall_mirror.resolve_manapool_cards((r.get("set") or "", r.get("number") or "") for r in singles)

    def res_for(r: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        return resolved.get(((r.get("set") or "").strip().upper(), str(r.get("number") or "").strip().lower()))

    refused = [r for r in singles if res_for(r) is None]
    accepted = [r for r in singles if res_for(r) is not None]

    tokens_items = [r for r in accepted if res_for(r)["is_token"]]
    promo_items = [r for r in accepted if not res_for(r)["is_token"] and r["set"].strip().upper() in PROMO_SETS]
    regular_sets: Dict[str, List[Dict[str, Any]]] = {}
    for r in accepted:
        if not res_for(r)["is_token"] and r["set"].strip().upper() not in PROMO_SETS:
            regular_sets.setdefault(r["set"].strip().upper(), []).append(r)

    unmapped_sets: List[str] = []

    def box_for(key: str) -> str:
        box = set_to_box.get(key)
        if not box:
            box = unmapped_box
            if key not in unmapped_sets:
                unmapped_sets.append(key)
        return box

    order: List[Tuple[Dict[str, Any], str, str, int]] = []
    for idx, it in enumerate(sorted(tokens_items, key=_token_sort_key), start=1):
        order.append((it, box_for(TOKENS_SET_KEY), TOKEN_SECTION, idx))
    promo_box = set_to_box.get("PROMOS", "M19")
    for idx, it in enumerate(sorted(promo_items, key=_promo_sort_key), start=1):
        order.append((it, promo_box, "PROMOS", idx))
    for set_code, items in sorted(regular_sets.items()):
        box = box_for(set_code)
        for idx, it in enumerate(sorted(items, key=_set_sort_key), start=1):
            order.append((it, box, "Sec-1", idx))

    placements: List[Dict[str, Any]] = []
    inventory_rows: List[Dict[str, Any]] = []
    new_layout: Dict[Tuple[str, str], List[ProductKey]] = {}
    section_sets: Dict[Tuple[str, str], List[str]] = {}
    box_stats: Dict[str, Dict[str, Any]] = {}
    for it, box, section, idx in order:
        res = res_for(it)
        placements.append({"row": it, "res": res, "box_label": box, "section_label": section, "index_num": idx})
        price_str = it.get("price") or it.get("market_price")
        try:
            price = float(price_str) if price_str else None
        except ValueError:
            price = None
        inventory_rows.append({
            "scryfall_id": res["front"]["scryfall_id"],
            "back_scryfall_id": res["back"]["scryfall_id"] if res["back"] else None,
            "manapool_product_id": (it.get("product_id") or "").strip(),
            "condition": (it.get("condition") or "NM").strip(),
            "is_foil": _is_foil(it),
            "quantity": int(it.get("quantity", 1)),
            "manapool_listed_price": price,
            "box_label": box,
            "section_label": section,
            "index_num": idx,
        })
        new_layout.setdefault((box, section), []).append(_product_key(it))
        section_sets.setdefault((box, section), []).append(it["set"].strip().upper())
        stats = box_stats.setdefault(box, {"sets": set(), "listings": 0, "cards": 0})
        stats["sets"].add(it["set"].strip().upper())
        stats["listings"] += 1
        stats["cards"] += int(it.get("quantity", 1))

    return {
        "placements": placements,
        "inventory_rows": inventory_rows,
        "new_layout": new_layout,
        "section_sets": section_sets,
        "box_stats": box_stats,
        "refused": refused,
        "skipped_non_singles": len(in_stock) - len(singles),
        "unmapped_sets": unmapped_sets,
    }


def import_live_inventory(
    inventory_csv: str,
    map_csv: str = "Manapool Print Format - Set_Box Map.csv",
    unmapped_box: str = "Unassigned",
    rebuild: bool = False,
    output_dir: str = "exports",
) -> Dict[str, Any]:
    """Imports a ManaPool inventory backup, mapped to physical boxes and the Scryfall mirror.

    Args:
        inventory_csv (str): Path to exported ManaPool inventory CSV.
        map_csv (str): Path to Set_Box map CSV.
        unmapped_box (str): Default box for sets (or TOKENS) without a mapped location.
        rebuild (bool): If True, drop and recreate the tables and insert. If False, preview only.
        output_dir (str): Directory for the summary file.

    Returns:
        Dict[str, Any]: Execution summary dictionary.
    """
    plan = plan_import(inventory_csv, map_csv, unmapped_box)
    inventory_rows = plan["inventory_rows"]
    tokens = [p for p in plan["placements"] if p["res"]["is_token"]]

    resort = build_resort_list(fetch_current_layout(), plan["new_layout"], plan["section_sets"])
    summary_path = write_summary_file(plan["refused"], resort, output_dir)

    if rebuild:
        rebuild_database(inventory_rows)

    summary_rows = []
    for b_label in sorted(plan["box_stats"].keys()):
        b_info = plan["box_stats"][b_label]
        sample_sets = ", ".join(sorted(b_info["sets"])[:4])
        if len(b_info["sets"]) > 4:
            sample_sets += f" (+{len(b_info['sets']) - 4} more)"
        summary_rows.append([b_label, len(b_info["sets"]), sample_sets, f"{b_info['listings']:,}", f"{b_info['cards']:,}"])

    return {
        "status": "success",
        "rebuilt": rebuild,
        "total_lines": len(inventory_rows),
        "total_cards": sum(r["quantity"] for r in inventory_rows),
        "tokens": len(tokens),
        "token_combos": sum(1 for p in tokens if p["res"]["back"]),
        "skipped_non_singles": plan["skipped_non_singles"],
        "refused": plan["refused"],
        "resort": resort,
        "unmapped_sets": plan["unmapped_sets"],
        "summary_path": summary_path,
        "summary_table": tabulate(
            summary_rows,
            headers=["Physical Box", "Sets Count", "Sample Sets", "Listings", "Total Cards"],
            tablefmt="fancy_grid",
        ),
    }

def main():
    """CLI entry point for import_live_inventory."""
    parser = argparse.ArgumentParser(description="Fresh import of ManaPool inventory against the Scryfall mirror")
    parser.add_argument(
        "--inventory-csv",
        required=True,
        help="Path to ManaPool inventory export CSV",
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
        "--rebuild",
        action="store_true",
        help="Drop cards/inventory/price_history, recreate from schema.sql and insert (otherwise preview only)",
    )
    parser.add_argument("--output-dir", default="exports", help="Directory for the import summary file")

    args = parser.parse_args()

    try:
        res = import_live_inventory(
            inventory_csv=args.inventory_csv,
            map_csv=args.map_csv,
            unmapped_box=args.unmapped_box,
            rebuild=args.rebuild,
            output_dir=args.output_dir,
        )

        print("\n" + res["summary_table"])
        mode_str = "[DATABASE REBUILT]" if res["rebuilt"] else "[PREVIEW - NO DB CHANGES; pass --rebuild to write]"
        print(f"\n{mode_str}")
        print(f"Total Physical Cards: {res['total_cards']:,} across {res['total_lines']:,} listings.")
        print(f"Tokens:               {res['tokens']:,} listings ({res['token_combos']} double-sided combos) in {TOKEN_SECTION}")
        print(f"Skipped non-singles:  {res['skipped_non_singles']}")
        print(f"Refused (no match):   {len(res['refused'])}")
        print(f"Order differs from DB: {len(res['resort'])} sections (see summary file)")
        print(f"Summary file:         {res['summary_path']}")

        if res["unmapped_sets"]:
            print(f"\nNote: {len(res['unmapped_sets'])} sets were not found in Set_Box map and placed into '{args.unmapped_box}':")
            print("  " + ", ".join(res["unmapped_sets"]))

    except Exception as e:
        print(f"Error during import: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
