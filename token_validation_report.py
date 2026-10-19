"""Token validation report: finds tokens and non-tokens that are filed in the wrong place.

Compares every in-stock inventory row against the local Scryfall mirror and lists:
    (a) tokens (by Scryfall layout) sitting in regular set sections,
    (b) non-tokens sitting in token sections (TOKENS / TOKEN-n),
    (c) rows that could not be matched to the mirror at all.

The report only reads; it never moves or changes inventory. Output is a printable
walk sheet (box -> set -> section -> index) plus a CSV with the same rows.

Works against both the legacy schema (inventory -> cards with synthetic IDs, matched
to the mirror by set code + collector number) and the current one (inventory ->
scryfall_cards directly).

Usage:
    python token_validation_report.py [--output-dir exports]
"""

import argparse
import csv
import datetime
import os
import re
import sys
from typing import Any, Dict, List, Tuple

from tabulate import tabulate

import db
import scryfall_mirror

TOKEN_SECTION_RE = re.compile(r"^TOKEN(S|-\d+)$", re.IGNORECASE)

CATEGORY_TITLES = {
    "token_in_set_box": "(a) TOKENS FILED IN SET SECTIONS - candidates to move into the token box",
    "non_token_in_token_box": "(b) NON-TOKENS FILED IN TOKEN SECTIONS - candidates to move back to their set",
    "unmatched": "(c) NOT FOUND IN SCRYFALL MIRROR - check set code / collector number",
}


def is_token_section(section_label: str) -> bool:
    """Returns True for token section labels (legacy 'TOKENS' or 'TOKEN-n')."""
    return bool(TOKEN_SECTION_RE.match((section_label or "").strip()))


def _legacy_cards_table_exists() -> bool:
    """Returns True while the pre-mirror `cards` table is still present."""
    with db.get_db_cursor(commit=False) as cur:
        cur.execute("SELECT to_regclass('public.cards') IS NOT NULL AS present;")
        return bool(cur.fetchone()["present"])


def fetch_inventory_rows() -> List[Dict[str, Any]]:
    """Loads in-stock inventory with set code, collector number and name.

    Returns:
        List[Dict[str, Any]]: Inventory rows with box/section/index and card identity.
    """
    catalog = "cards" if _legacy_cards_table_exists() else "scryfall_cards"
    query = f"""
        SELECT i.inventory_id, i.box_label, i.section_label, i.index_num,
               i.is_foil, i.quantity, c.name, c.set_code, c.collector_number
        FROM inventory i
        JOIN {catalog} c ON i.scryfall_id = c.scryfall_id
        WHERE i.quantity > 0;
    """
    with db.get_db_cursor(commit=False) as cur:
        cur.execute(query)
        return [dict(r) for r in cur.fetchall()]


def classify_rows(
    rows: List[Dict[str, Any]],
    mirror: Dict[Tuple[str, str], Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Flags rows whose physical location disagrees with their Scryfall layout.

    Args:
        rows (List[Dict[str, Any]]): Inventory rows from `fetch_inventory_rows`.
        mirror (Dict[Tuple[str, str], Dict[str, Any]]): Output of `scryfall_mirror.lookup_cards`.

    Returns:
        List[Dict[str, Any]]: Flagged rows, each with 'category' and 'layout' added.
    """
    flagged: List[Dict[str, Any]] = []
    for r in rows:
        key = ((r["set_code"] or "").strip().upper(), str(r["collector_number"] or "").strip().lower())
        card = mirror.get(key)
        in_token_section = is_token_section(r["section_label"])
        if card is None:
            category = "unmatched"
            layout = ""
        else:
            layout = card["layout"]
            is_token = scryfall_mirror.is_token_layout(layout)
            if is_token and not in_token_section:
                category = "token_in_set_box"
            elif in_token_section and not is_token:
                category = "non_token_in_token_box"
            else:
                continue
        flagged.append({**r, "category": category, "layout": layout})
    return flagged


def _walk_key(r: Dict[str, Any]) -> Tuple[Any, ...]:
    """Physical walk order: box (natural) -> set -> section (natural) -> index."""
    m_box = re.search(r"(\d+)", r["box_label"] or "")
    m_sec = re.search(r"(\d+)", r["section_label"] or "")
    return (
        int(m_box.group(1)) if m_box else 999,
        r["box_label"] or "",
        (r["set_code"] or "").upper(),
        int(m_sec.group(1)) if m_sec else 0,
        r["section_label"] or "",
        r["index_num"] or 0,
    )


def write_report(flagged: List[Dict[str, Any]], total_rows: int, output_dir: str = "exports") -> Tuple[str, str]:
    """Writes the walk sheet (.txt) and CSV for flagged rows.

    Args:
        flagged (List[Dict[str, Any]]): Output of `classify_rows`.
        total_rows (int): Number of inventory rows checked.
        output_dir (str): Destination folder.

    Returns:
        Tuple[str, str]: Absolute paths to the text report and the CSV.
    """
    os.makedirs(output_dir, exist_ok=True)
    today_str = datetime.date.today().isoformat()
    txt_path = os.path.join(output_dir, f"token_validation_{today_str}.txt")
    csv_path = os.path.join(output_dir, f"token_validation_{today_str}.csv")
    flagged = sorted(flagged, key=_walk_key)

    counts = {c: sum(1 for r in flagged if r["category"] == c) for c in CATEGORY_TITLES}
    header = f"""========================================================================================
                          TOKEN VALIDATION REPORT (READ ONLY)
========================================================================================
 Date:       {today_str}
 Checked:    {total_rows:,} in-stock inventory rows against scryfall_cards
 Rule:       token  <=>  Scryfall layout in {{{', '.join(sorted(scryfall_mirror.TOKEN_LAYOUTS))}}}
 Found:      (a) {counts['token_in_set_box']}   (b) {counts['non_token_in_token_box']}   (c) {counts['unmatched']}
 Path:       Sorted by Box -> Set -> Section -> Index
 Note:       Nothing has been moved. Verify each card physically before relocating it.
========================================================================================
"""
    body: List[str] = []
    for category, title in CATEGORY_TITLES.items():
        rows = [r for r in flagged if r["category"] == category]
        body.append(f"\n{title}\n")
        if not rows:
            body.append("  (none)\n")
            continue
        body.append(tabulate(
            [[
                r["box_label"],
                r["set_code"],
                r["section_label"],
                f"#{r['index_num']:03d}",
                (r["name"] or "")[:34],
                r["collector_number"],
                "FOIL" if r["is_foil"] else "NF",
                r["quantity"],
                r["layout"] or "-",
                "[   ]",
            ] for r in rows],
            headers=["Box", "Set", "Section", "Index", "Card Name", "Num", "Finish", "Qty", "Layout", "Checked"],
            tablefmt="simple",
        ) + "\n")

    with open(txt_path, "w", encoding="utf-8") as f:
        f.write(header)
        f.write("".join(body))
        f.write("========================================================================================\n")

    fields = ["category", "box_label", "set_code", "section_label", "index_num", "name",
              "collector_number", "is_foil", "quantity", "layout", "inventory_id"]
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(flagged)

    return os.path.abspath(txt_path), os.path.abspath(csv_path)


def run_report(output_dir: str = "exports") -> Dict[str, Any]:
    """Builds the token validation report from the live database.

    Args:
        output_dir (str): Destination folder.

    Returns:
        Dict[str, Any]: Summary with per-category counts and output paths.

    Raises:
        RuntimeError: If the Scryfall mirror is empty.
    """
    if scryfall_mirror.get_mirror_status()["card_count"] == 0:
        raise RuntimeError("scryfall_cards is empty; run 'python scryfall_mirror.py refresh' first.")
    rows = fetch_inventory_rows()
    mirror = scryfall_mirror.lookup_cards((r["set_code"], str(r["collector_number"])) for r in rows)
    flagged = classify_rows(rows, mirror)
    txt_path, csv_path = write_report(flagged, len(rows), output_dir)
    counts = {c: sum(1 for r in flagged if r["category"] == c) for c in CATEGORY_TITLES}
    return {"checked": len(rows), "counts": counts, "txt_path": txt_path, "csv_path": csv_path}


def main() -> None:
    """CLI entry point for the token validation report."""
    parser = argparse.ArgumentParser(description="Report tokens / non-tokens filed in the wrong place (read only)")
    parser.add_argument("--output-dir", default="exports", help="Directory for the report files")
    args = parser.parse_args()

    result = run_report(args.output_dir)
    c = result["counts"]
    print(f"Checked {result['checked']:,} rows: (a) {c['token_in_set_box']} tokens in set sections, "
          f"(b) {c['non_token_in_token_box']} non-tokens in token sections, (c) {c['unmatched']} unmatched.")
    print(f"Walk sheet: {result['txt_path']}")
    print(f"CSV:        {result['csv_path']}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
