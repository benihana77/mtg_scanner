"""Special-case check sheet: a printable pull list of every unusual card in inventory.

Runs the fresh-import plan (read only) and lists each card that hits a known edge case,
at the box / section / index the import assigns it, with what to check. Use it to walk
the shelves and confirm each one is physically where the database will say it is.

Usage:
    python -m tools.special_cases_report [--inventory-csv ...] [--output-dir exports]
"""

import argparse
import csv
import datetime
import os
import re
import sys
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from tabulate import tabulate

from station import SET_BOX_MAP_CSV
from station import import_live_inventory as ili

# Check code -> what it means / what to verify. Order is the legend order.
CHECKS: Dict[str, str] = {
    "COMBO": "Double-sided token sold by ManaPool as one product (two Scryfall faces). Check both faces match the name.",
    "DFC-TOKEN": "Double-faced token. Check it is with the tokens, not its set.",
    "EMBLEM": "Emblem. Filed with tokens.",
    "HELPER": "Token-layout card without a 'Token' type line (The Monarch, Day // Night, Morph). Filed with tokens.",
    "TOKEN-FROM-SET": "Token printed in a non-token set (e.g. Duel Deck). Filed with tokens, not its set.",
    "NONTOKEN-IN-TSET": "Non-token card from a token set. Filed in its set's box, not with tokens.",
    "PROMO": "Standalone promo. PROMOS section.",
    "NAME-DIFF": "ManaPool name differs from Scryfall (e.g. meld pair). Check the printing.",
    "SORT-SYMBOL": "Name starts with a symbol/punctuation. Filed at the FRONT of its section.",
    "SORT-ACCENT": "Name starts with an accented letter. Filed under its base letter (\u00c9omer under E).",
    "VARIANT-ORDER": "Same-name printings: regular printing (lowest number) first, then showcase/other variants.",
    "THE-LIST": "The List (PLST): collector number embeds the original set. Listed in the appendix.",
    "UNASSIGNED": "Set has no box in the Set/Box map.",
}


def _variant_order_keys(placements: List[Dict[str, Any]]) -> set:
    """Finds same-name groups whose numeric collector order differs from plain text order (7 vs 302)."""
    groups: Dict[Tuple[str, str, str, str], List[Dict[str, Any]]] = defaultdict(list)
    for p in placements:
        key = (p["box_label"], p["section_label"], p["row"]["set"].strip().upper(), p["row"]["name"].strip().lower())
        groups[key].append(p)

    flagged = set()
    for members in groups.values():
        nums = sorted({m["row"]["number"].strip().lower() for m in members})
        if len(nums) > 1 and nums != sorted(nums, key=ili.collector_number_sort_key):
            flagged.update(id(m) for m in members)
    return flagged


def classify(placements: List[Dict[str, Any]], unmapped_box: str = "Unassigned") -> List[Dict[str, Any]]:
    """Tags each placement with the special-case checks it triggers.

    Args:
        placements (List[Dict[str, Any]]): `plan_import()['placements']`.
        unmapped_box (str): Box label used for unmapped sets.

    Returns:
        List[Dict[str, Any]]: Placements with a non-empty 'checks' list added.
    """
    variant_ids = _variant_order_keys(placements)
    out: List[Dict[str, Any]] = []
    for p in placements:
        row, res = p["row"], p["res"]
        front = res["front"]
        name = row["name"].strip()
        checks: List[str] = []
        if res["back"]:
            checks.append("COMBO")
        if front["layout"] == "double_faced_token":
            checks.append("DFC-TOKEN")
        if front["layout"] == "emblem":
            checks.append("EMBLEM")
        if res["is_token"] and front["layout"] != "emblem" and "token" not in (front.get("type_line") or "").lower():
            checks.append("HELPER")
        if res["is_token"] and front.get("set_type") != "token":
            checks.append("TOKEN-FROM-SET")
        if not res["is_token"] and front.get("set_type") == "token":
            checks.append("NONTOKEN-IN-TSET")
        if p["section_label"] == "PROMOS":
            checks.append("PROMO")
        if not res["back"] and name.lower() != res["name"].strip().lower():
            checks.append("NAME-DIFF")
        first = name[:1]
        if first and not first.isalnum():
            checks.append("SORT-SYMBOL")
        elif first and not first.isascii():
            checks.append("SORT-ACCENT")
        if id(p) in variant_ids:
            checks.append("VARIANT-ORDER")
        if row["set"].strip().upper() == "PLST":
            checks.append("THE-LIST")
        if p["box_label"] == unmapped_box:
            checks.append("UNASSIGNED")
        if checks:
            out.append({**p, "checks": checks})
    return out


def _walk_key(p: Dict[str, Any]) -> Tuple[Any, ...]:
    """Box (natural) -> set -> section -> index; multi-set sections (TOKEN-n, PROMOS) walk straight down."""
    m_box = re.search(r"(\d+)", p["box_label"])
    section = p["section_label"].upper()
    multi_set = section.startswith("TOKEN") or section == "PROMOS"
    return (
        int(m_box.group(1)) if m_box else 999,
        p["box_label"],
        section if multi_set else p["row"]["set"].strip().upper(),
        p["section_label"],
        p["index_num"],
    )


def _table(items: List[Dict[str, Any]]) -> str:
    return tabulate(
        [[
            p["box_label"],
            p["section_label"],
            f"#{p['index_num']:03d}",
            p["row"]["set"],
            p["row"]["number"],
            p["row"]["name"][:38],
            "FOIL" if ili._is_foil(p["row"]) else "NF",
            p["row"].get("quantity", ""),
            ", ".join(c for c in p["checks"] if c != "THE-LIST") or "THE-LIST",
            "[   ]",
        ] for p in items],
        headers=["Box", "Section", "Index", "Set", "Num", "Card Name", "Fin", "Qty", "Check", "OK"],
        tablefmt="simple",
    )


def write_sheet(flagged: List[Dict[str, Any]], inventory_csv: str, output_dir: str) -> Tuple[str, str]:
    """Writes the printable check sheet (.txt) and a CSV copy.

    Returns:
        Tuple[str, str]: Absolute paths to the text sheet and the CSV.
    """
    os.makedirs(output_dir, exist_ok=True)
    today = datetime.date.today().isoformat()
    txt_path = os.path.join(output_dir, f"special_cases_{today}.txt")
    csv_path = os.path.join(output_dir, f"special_cases_{today}.csv")

    flagged = sorted(flagged, key=_walk_key)
    main_items = [p for p in flagged if p["checks"] != ["THE-LIST"]]
    list_items = [p for p in flagged if "THE-LIST" in p["checks"]]
    counts = {c: sum(1 for p in flagged if c in p["checks"]) for c in CHECKS}

    legend = "\n".join(f" {c:<17}({counts[c]:>3})  {desc}" for c, desc in CHECKS.items() if counts[c])
    header = f"""==========================================================================================================
                              SPECIAL-CASE PHYSICAL CHECK SHEET
==========================================================================================================
 Date:       {today}
 Source:     {inventory_csv} (fresh-import plan; locations are where the database WILL put each card)
 Listed:     {len(main_items)} cards below + {len(list_items)} The List cards in the appendix
 Path:       Box -> Set -> Section -> Index (TOKEN-n and PROMOS walked straight down their index)

 Before you start:
  - Box M19 token divider 'TOKENS' becomes 'TOKEN-1' (same cards, same order). Relabel it.
  - The Oct 7 MOM test batch is NOT part of inventory. Keep it physically separate.
  - Tick OK when the card is at that Box / Section / Index. Note anything that is not.

 Check codes:
{legend}
==========================================================================================================

"""
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(header)
        f.write(_table(main_items) if main_items else "  (none)")
        f.write("\n\n==========================================================================================================\n")
        f.write(f" APPENDIX: THE LIST (PLST), {len(list_items)} cards. Collector numbers like 'CN2-126' are exact Scryfall matches.\n")
        f.write("==========================================================================================================\n")
        f.write(_table(list_items) if list_items else "  (none)")
        f.write("\n")

    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["box_label", "section_label", "index_num", "set", "number", "name", "finish",
                         "quantity", "checks", "scryfall_name", "scryfall_id", "back_scryfall_id"])
        for p in flagged:
            writer.writerow([
                p["box_label"], p["section_label"], p["index_num"], p["row"]["set"], p["row"]["number"],
                p["row"]["name"], p["row"].get("finish"), p["row"].get("quantity"), " ".join(p["checks"]),
                p["res"]["name"], p["res"]["front"]["scryfall_id"],
                p["res"]["back"]["scryfall_id"] if p["res"]["back"] else "",
            ])
    return os.path.abspath(txt_path), os.path.abspath(csv_path)


def main() -> None:
    """CLI entry point for the special-case check sheet."""
    parser = argparse.ArgumentParser(description="Printable check sheet for unusual cards (read only)")
    parser.add_argument("--inventory-csv", required=True, help="Path to ManaPool inventory export CSV")
    parser.add_argument("--map-csv", default=SET_BOX_MAP_CSV)
    parser.add_argument("--output-dir", default="exports")
    args = parser.parse_args()

    plan = ili.plan_import(args.inventory_csv, args.map_csv)
    flagged = classify(plan["placements"])
    txt_path, csv_path = write_sheet(flagged, args.inventory_csv, args.output_dir)
    if plan["refused"]:
        print(f"Warning: {len(plan['refused'])} rows were refused by the import plan; see the import summary.")
    print(f"Flagged {len(flagged)} listings.")
    print(f"Check sheet: {txt_path}")
    print(f"CSV:         {csv_path}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
