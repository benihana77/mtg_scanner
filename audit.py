"""Physical inventory audit: count sheets, count import, and the final ManaPool quantity push.

Audit parts (see `generate`):
    (a) DRIFT     listings where ManaPool and the database disagree, sampled per set (weighted
                  toward large quantities); sets listed in --full-sets are counted in full
    (b) BASELINE  a random sample of listings where ManaPool and the database agree
    (c) VALUE     every in-stock listing priced at or above --value-threshold

Counts are recorded per physical row (box / section / index). After filling the `counted`
column of the CSV (blank = not counted), `apply` sets each counted row's database quantity,
appends the result to audits/applied_counts.csv, and reports overcounts (database higher than
the shelf). A set whose DRIFT sample has any overcount gets a follow-up sheet for a full count
of its remaining drifted rows.

`push` compares the audited database with live ManaPool and, with --send, pushes the
differing quantities (quantity only, prices untouched) and reads every one back.

Usage:
    python audit.py generate [--per-set 8] [--baseline 100] [--value-threshold 1.00] [--full-sets SOI]
    python audit.py apply audits/audit_YYYY-MM-DD.csv
    python audit.py push [--send]
"""

import argparse
import csv
import datetime
import os
import random
import re
import sys
import zipfile
import xml.etree.ElementTree as ET
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from tabulate import tabulate

import db
import manapool_client

AUDIT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "audits")
APPLIED_LOG = os.path.join(AUDIT_DIR, "applied_counts.csv")
FIELDS = ["part", "box", "section", "index", "set", "number", "name", "finish", "db_qty", "manapool_qty",
          "product_id", "inventory_id", "counted"]


def load_rows() -> List[Dict[str, Any]]:
    """Every inventory row with its card, location, product and the product's database total."""
    query = """
        SELECT i.inventory_id, i.manapool_product_id AS product_id, i.box_label, i.section_label, i.index_num,
               i.quantity, i.is_foil, i.manapool_listed_price AS price,
               sc.name || COALESCE(' // ' || b.name, '') AS name, sc.set_code,
               sc.collector_number || COALESCE('/' || b.collector_number, '') AS collector_number,
               SUM(i.quantity) OVER (PARTITION BY i.manapool_product_id) AS product_total
        FROM inventory i
        JOIN scryfall_cards sc ON i.scryfall_id = sc.scryfall_id
        LEFT JOIN scryfall_cards b ON i.back_scryfall_id = b.scryfall_id
        WHERE i.manapool_product_id IS NOT NULL AND i.manapool_product_id <> '';
    """
    with db.get_db_cursor(commit=False) as cur:
        cur.execute(query)
        return [dict(r) for r in cur.fetchall()]


def live_quantities() -> Dict[str, int]:
    """product_id -> live ManaPool quantity."""
    return {i["product_id"]: i.get("quantity") or 0 for i in manapool_client.fetch_seller_inventory()}


def already_counted() -> set:
    """inventory_ids counted in earlier `apply` runs."""
    if not os.path.isfile(APPLIED_LOG):
        return set()
    with open(APPLIED_LOG, encoding="utf-8") as f:
        return {int(r["inventory_id"]) for r in csv.DictReader(f)}


def _walk_key(r: Dict[str, Any]) -> Tuple[Any, ...]:
    m_box = re.search(r"(\d+)", r["box"])
    m_sec = re.search(r"(\d+)", r["section"])
    multi = r["section"].upper().startswith("TOKEN") or r["section"].upper() == "PROMOS"
    return (int(m_box.group(1)) if m_box else 999, r["box"], r["section"] if multi else r["set"],
            int(m_sec.group(1)) if m_sec else 0, int(r["index"]))


def _sheet_row(r: Dict[str, Any], part: str, live_qty: Optional[int]) -> Dict[str, Any]:
    return {
        "part": part, "box": r["box_label"], "section": r["section_label"], "index": r["index_num"],
        "set": r["set_code"], "number": r["collector_number"], "name": r["name"],
        "finish": "FOIL" if r["is_foil"] else "NF", "db_qty": r["quantity"],
        "manapool_qty": "" if live_qty is None else live_qty,
        "product_id": r["product_id"], "inventory_id": r["inventory_id"], "counted": "",
    }


def _weighted_sample(rows: List[Dict[str, Any]], k: int, rng: random.Random) -> List[Dict[str, Any]]:
    """k rows without replacement, weighted by quantity (bigger stacks are likelier to hide miscounts)."""
    pool = list(rows)
    picked: List[Dict[str, Any]] = []
    while pool and len(picked) < k:
        total = sum(max(r["quantity"], 1) for r in pool)
        x = rng.uniform(0, total)
        for i, r in enumerate(pool):
            x -= max(r["quantity"], 1)
            if x <= 0:
                picked.append(pool.pop(i))
                break
        else:
            picked.append(pool.pop())
    return picked


def select_audit(
    rows: List[Dict[str, Any]],
    live: Dict[str, int],
    per_set: int = 8,
    baseline: int = 100,
    value_threshold: float = 1.00,
    full_sets: Optional[List[str]] = None,
    skip_ids: Optional[set] = None,
    seed: int = 20261019,
) -> List[Dict[str, Any]]:
    """Chooses the rows to count for parts DRIFT, BASELINE and VALUE.

    Args:
        rows: Output of `load_rows`.
        live: product_id -> live ManaPool quantity.
        per_set: DRIFT rows to sample per set.
        baseline: BASELINE rows to sample from non-drifted, in-stock rows.
        value_threshold: VALUE includes every in-stock row priced at or above this (dollars).
        full_sets: Set codes whose drifted rows are all counted.
        skip_ids: inventory_ids already counted (left out).
        seed: Random seed, so a sheet can be regenerated identically.

    Returns:
        List[Dict[str, Any]]: Sheet rows (FIELDS) in walk order; a row appears once, in its first part.
    """
    rng = random.Random(seed)
    skip_ids = skip_ids or set()
    full = {s.upper() for s in (full_sets or [])}
    in_stock = [r for r in rows if r["quantity"] > 0 and r["inventory_id"] not in skip_ids]
    drifted = [r for r in rows if r["product_id"] in live and live[r["product_id"]] != r["product_total"]
               and r["inventory_id"] not in skip_ids]

    chosen: Dict[int, Tuple[str, Dict[str, Any]]] = {}
    by_set: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in drifted:
        by_set[r["set_code"]].append(r)
    for set_code, set_rows in sorted(by_set.items()):
        picks = set_rows if set_code in full else _weighted_sample(sorted(set_rows, key=lambda r: r["inventory_id"]), per_set, rng)
        for r in picks:
            chosen.setdefault(r["inventory_id"], ("DRIFT-FULL" if set_code in full else "DRIFT", r))

    drifted_ids = {r["inventory_id"] for r in drifted}
    clean = sorted([r for r in in_stock if r["inventory_id"] not in drifted_ids and r["product_id"] in live],
                   key=lambda r: r["inventory_id"])
    for r in rng.sample(clean, min(baseline, len(clean))):
        chosen.setdefault(r["inventory_id"], ("BASELINE", r))

    for r in in_stock:
        if r["price"] is not None and float(r["price"]) >= value_threshold:
            chosen.setdefault(r["inventory_id"], ("VALUE", r))

    sheet = [_sheet_row(r, part, live.get(r["product_id"])) for part, r in chosen.values()]
    return sorted(sheet, key=_walk_key)


def write_sheet(sheet: List[Dict[str, Any]], name: str, title: str) -> Tuple[str, str]:
    """Writes the count sheet as a printable walk-order .txt and a fillable .csv in audits/."""
    os.makedirs(AUDIT_DIR, exist_ok=True)
    csv_path = os.path.join(AUDIT_DIR, f"{name}.csv")
    txt_path = os.path.join(AUDIT_DIR, f"{name}.txt")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS)
        writer.writeheader()
        writer.writerows(sheet)
    parts = defaultdict(int)
    for r in sheet:
        parts[r["part"]] += 1
    with open(txt_path, "w", encoding="utf-8") as f:
        f.write("=" * 106 + f"\n {title}\n" + "=" * 106 + "\n")
        f.write(f" Rows: {len(sheet)} ({', '.join(f'{k} {v}' for k, v in sorted(parts.items()))})\n")
        f.write(" Count the copies at each Box / Section / Index and write the number in the 'counted' column of\n")
        f.write(f" {os.path.basename(csv_path)} (leave blank if not counted), then run: python audit.py apply {os.path.relpath(csv_path)}\n")
        f.write("=" * 106 + "\n\n")
        f.write(tabulate([[r["box"], r["section"], f"#{int(r['index']):03d}", r["set"], r["number"], r["name"][:32],
                           r["finish"], r["db_qty"], r["manapool_qty"], r["part"], "[    ]"] for r in sheet],
                         headers=["Box", "Section", "Index", "Set", "Num", "Card", "Fin", "DB", "ManaPool", "Part", "Counted"],
                         tablefmt="simple") + "\n")
    return txt_path, csv_path


def read_sheet_rows(path: str) -> List[Dict[str, str]]:
    """Reads a filled-in count sheet from .csv or .xlsx (first worksheet; standard library only)."""
    if not path.lower().endswith(".xlsx"):
        with open(path, encoding="utf-8-sig") as f:
            return list(csv.DictReader(f))
    ns = {"m": "http://schemas.openxmlformats.org/spreadsheetml/2006/main",
          "r": "http://schemas.openxmlformats.org/officeDocument/2006/relationships"}
    with zipfile.ZipFile(path) as z:
        shared = []
        if "xl/sharedStrings.xml" in z.namelist():
            for si in ET.fromstring(z.read("xl/sharedStrings.xml")).findall("m:si", ns):
                shared.append("".join(t.text or "" for t in si.iter(f"{{{ns['m']}}}t")))
        first = ET.fromstring(z.read("xl/workbook.xml")).find("m:sheets", ns)[0]
        rels = {r.get("Id"): r.get("Target") for r in ET.fromstring(z.read("xl/_rels/workbook.xml.rels"))}
        target = rels[first.get(f"{{{ns['r']}}}id")].lstrip("/")
        sheet = ET.fromstring(z.read(target if target.startswith("xl/") else f"xl/{target}"))
    table: List[List[str]] = []
    for row in sheet.iter(f"{{{ns['m']}}}row"):
        cells: Dict[int, str] = {}
        for c in row.findall("m:c", ns):
            col = 0
            for ch in re.match(r"[A-Z]+", c.get("r")).group():
                col = col * 26 + ord(ch) - 64
            v, kind = c.find("m:v", ns), c.get("t")
            if kind == "inlineStr":
                val = "".join(t.text or "" for t in c.iter(f"{{{ns['m']}}}t"))
            elif v is None:
                val = ""
            else:
                val = shared[int(v.text)] if kind == "s" else v.text
            cells[col - 1] = val
        table.append([cells.get(i, "") for i in range(max(cells) + 1)] if cells else [])
    header = table[0]
    return [dict(zip(header, r + [""] * (len(header) - len(r)))) for r in table[1:] if any(r)]


def _whole_number(raw: str) -> Optional[int]:
    """'3', '3.0' (as Excel writes it) -> 3; anything else -> None."""
    try:
        f = float(raw)
    except ValueError:
        return None
    return int(f) if f >= 0 and f == int(f) else None


def apply_counts(csv_path: str) -> Dict[str, Any]:
    """Imports counted quantities: updates database rows, logs them, and finds sets needing a full count.

    Returns:
        Dict[str, Any]: 'applied' rows (with previous and counted qty), 'overcount_sets' (DRIFT parts),
            'baseline' stats, 'skipped' rows (blank or invalid counts).
    """
    rows = read_sheet_rows(csv_path)
    applied, skipped = [], []
    with db.get_db_cursor() as cur:
        for r in rows:
            raw = (r.get("counted") or "").strip()
            if not raw:
                continue
            counted = _whole_number(raw)
            inventory_id = _whole_number(str(r.get("inventory_id") or ""))
            if counted is None or inventory_id is None:
                skipped.append({**r, "reason": f"not a whole number: {raw!r}"})
                continue
            cur.execute("SELECT quantity FROM inventory WHERE inventory_id = %s;", (inventory_id,))
            found = cur.fetchone()
            if not found:
                skipped.append({**r, "reason": "inventory row no longer exists"})
                continue
            cur.execute("UPDATE inventory SET quantity = %s WHERE inventory_id = %s;", (counted, inventory_id))
            applied.append({**r, "inventory_id": inventory_id, "previous_qty": found["quantity"], "counted": counted})

    os.makedirs(AUDIT_DIR, exist_ok=True)
    new_log = not os.path.isfile(APPLIED_LOG)
    with open(APPLIED_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        if new_log:
            writer.writerow(["applied_at", "source", "part", "inventory_id", "product_id", "set", "number", "name",
                             "finish", "previous_qty", "counted"])
        stamp = datetime.datetime.now().isoformat(timespec="seconds")
        for a in applied:
            writer.writerow([stamp, os.path.basename(csv_path), a["part"], a["inventory_id"], a["product_id"], a["set"],
                             a["number"], a["name"], a["finish"], a["previous_qty"], a["counted"]])

    over_sets = sorted({a["set"] for a in applied if a["part"] == "DRIFT" and a["counted"] < a["previous_qty"]})
    base = [a for a in applied if a["part"] == "BASELINE"]
    baseline = {
        "counted": len(base),
        "exact": sum(1 for a in base if a["counted"] == a["previous_qty"]),
        "over": sum(1 for a in base if a["counted"] < a["previous_qty"]),
        "under": sum(1 for a in base if a["counted"] > a["previous_qty"]),
    }
    return {"applied": applied, "overcount_sets": over_sets, "baseline": baseline, "skipped": skipped}


def plan_push(rows: Optional[List[Dict[str, Any]]] = None, live: Optional[Dict[str, int]] = None) -> Dict[str, int]:
    """product_id -> database total for every listing where live ManaPool quantity differs."""
    rows = rows if rows is not None else load_rows()
    live = live if live is not None else live_quantities()
    totals: Dict[str, int] = {}
    for r in rows:
        totals[r["product_id"]] = r["product_total"]
    return {pid: qty for pid, qty in totals.items() if pid in live and live[pid] != qty}


def main() -> None:
    """CLI entry point for the audit."""
    parser = argparse.ArgumentParser(description="Physical inventory audit")
    sub = parser.add_subparsers(dest="command", required=True)
    p_gen = sub.add_parser("generate", help="Write the audit count sheet (txt + fillable csv)")
    p_gen.add_argument("--per-set", type=int, default=8)
    p_gen.add_argument("--baseline", type=int, default=100)
    p_gen.add_argument("--value-threshold", type=float, default=1.00)
    p_gen.add_argument("--full-sets", nargs="*", default=["SOI"])
    p_apply = sub.add_parser("apply", help="Import a filled-in count CSV")
    p_apply.add_argument("csv_file", help="Filled-in count sheet (.csv, or .xlsx saved from Excel)")
    p_push = sub.add_parser("push", help="Push audited database quantities to ManaPool (dry run unless --send)")
    p_push.add_argument("--send", action="store_true")
    args = parser.parse_args()

    if args.command == "generate":
        sheet = select_audit(load_rows(), live_quantities(), args.per_set, args.baseline, args.value_threshold,
                             args.full_sets, already_counted())
        name = f"audit_{datetime.date.today().isoformat()}"
        txt, csv_path = write_sheet(sheet, name, "INVENTORY AUDIT COUNT SHEET")
        print(f"{len(sheet)} rows to count.\nPrint: {txt}\nFill:  {csv_path}")

    elif args.command == "apply":
        res = apply_counts(args.csv_file)
        changed = [a for a in res["applied"] if a["counted"] != a["previous_qty"]]
        print(f"Applied {len(res['applied'])} counts; {len(changed)} changed the database:")
        if changed:
            print(tabulate([[a["set"], a["number"], a["name"][:30], a["finish"], a["part"], a["previous_qty"], a["counted"],
                             "OVER" if a["counted"] < a["previous_qty"] else "under"] for a in changed],
                           headers=["Set", "Num", "Card", "Fin", "Part", "DB was", "Counted", "Direction"], tablefmt="simple"))
        for s in res["skipped"]:
            print(f"[skipped] {s.get('set')} {s.get('number')} {s.get('name')}: {s['reason']}", file=sys.stderr)
        b = res["baseline"]
        if b["counted"]:
            print(f"Baseline: {b['exact']}/{b['counted']} exact, {b['over']} overcounts "
                  f"({b['over'] / b['counted'] * 100:.1f}%), {b['under']} undercounts.")
        if res["overcount_sets"]:
            rows, live = load_rows(), live_quantities()
            follow = [r for r in select_audit(rows, live, per_set=0, baseline=0, value_threshold=float("inf"),
                                              full_sets=res["overcount_sets"], skip_ids=already_counted())]
            name = f"audit_followup_{datetime.date.today().isoformat()}_{'_'.join(res['overcount_sets'])}"
            txt, csv_path = write_sheet(follow, name, f"FOLLOW-UP FULL COUNT: {', '.join(res['overcount_sets'])}")
            print(f"Overcounts found in {', '.join(res['overcount_sets'])}: full count of {len(follow)} remaining "
                  f"drifted rows.\nPrint: {txt}\nFill:  {csv_path}")

    elif args.command == "push":
        quantities = plan_push()
        print(f"{len(quantities)} listings differ between the audited database and ManaPool "
              f"(net {sum(quantities.values()) - sum(live_quantities().get(p, 0) for p in quantities):+d} copies).")
        if not args.send:
            print("[DRY RUN - nothing sent] Re-run with --send to push these quantities.")
            return
        updated, errors = manapool_client.push_quantities(quantities)
        for e in errors:
            print(f"[ManaPool error] {e}", file=sys.stderr)
        remaining = plan_push()
        print(f"Pushed {updated}; read back: {len(remaining)} listings still differ.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
