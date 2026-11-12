#!/usr/bin/env python3
"""Curses-based Terminal User Interface (TUI) for MTG Inventory Management.

Provides an interactive console dashboard for:
N. Building an intake sheet for one set from ManaPool's catalog, counting it, and ingesting it.
1. Ingesting ManaPool Set CSV batches (with alphabetical sorting and index numbering).
2. Pushing inventory updates directly to the ManaPool API (or generating upload CSVs).
3. Pulling live orders from ManaPool and generating optimized pick traversal sheets.
4. Processing local picklist CSV files.
5. Browsing physical inventory breakdown (Boxes, Sets, Sections, Index ranges).
6. Moving a whole set (every section), or every TOKENS chunk, to another box (Box Shuffle when space runs out).
7. Connection diagnostics.
8. Refreshing the local Scryfall mirror (scryfall_cards).
9. Pulling cards for a personal collection / deck (search, decklist, ManaPool sync).
R. Repricing listings from ManaPool market data (preview, review, push).
P. Managing repricer price pins.
"""

import curses
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

from station import db
from station import collection_pull
from station import csv_pipeline
from station import intake_sheet
from station import manapool_client
from station import pull_list_optimizer
from station import repricer
from station import scryfall_mirror


def prompt_input(stdscr, prompt_text: str, default_val: str = "") -> str:
    """Displays a prompt and captures user keyboard input with backspace support.

    Args:
        stdscr: Curses screen.
        prompt_text (str): Prompt instruction.
        default_val (str): Initial string value.

    Returns:
        str: User input string.
    """
    curses.echo()
    curses.curs_set(1)
    max_y, max_x = stdscr.getmaxyx()
    y = max_y - 3
    
    # Clear input row
    stdscr.move(y, 2)
    stdscr.clrtoeol()
    full_prompt = f"{prompt_text} [{default_val}]: " if default_val else f"{prompt_text}: "
    stdscr.addstr(y, 2, full_prompt[: max_x - 4], curses.color_pair(3) | curses.A_BOLD)
    stdscr.refresh()

    try:
        user_bytes = stdscr.getstr(y, 2 + len(full_prompt), max_x - len(full_prompt) - 4)
        val = user_bytes.decode("utf-8").strip()
        result = val if val else default_val
    except Exception:
        result = default_val
    finally:
        curses.noecho()
        curses.curs_set(0)

    return result


def show_message_box(stdscr, title: str, lines: List[str], wait_key: bool = True) -> None:
    """Displays a centered message modal dialog.

    Args:
        stdscr: Curses screen.
        title (str): Modal header title.
        lines (List[str]): Content lines to display.
        wait_key (bool): Whether to wait for operator keypress before dismissing.
    """
    max_y, max_x = stdscr.getmaxyx()
    box_height = min(max_y - 4, len(lines) + 6)
    box_width = min(max_x - 6, max(len(l) for l in lines + [title, "Press any key to continue..."]) + 8)
    box_width = max(box_width, 40)

    start_y = max(1, (max_y - box_height) // 2)
    start_x = max(1, (max_x - box_width) // 2)

    win = curses.newwin(box_height, box_width, start_y, start_x)
    win.box()
    win.bkgd(" ", curses.color_pair(1))

    # Header
    win.addstr(1, 2, f" {title} "[: box_width - 4], curses.color_pair(2) | curses.A_BOLD)

    # Content
    for idx, line in enumerate(lines[: box_height - 5]):
        win.addstr(3 + idx, 2, line[: box_width - 4], curses.color_pair(1))

    if wait_key:
        prompt_str = "[ Press Any Key to Continue ]"
        win.addstr(box_height - 2, max(2, (box_width - len(prompt_str)) // 2), prompt_str, curses.color_pair(3) | curses.A_BOLD)
        win.refresh()
        win.getch()
    else:
        win.refresh()


def draw_header(stdscr, status_msg: str = "Ready") -> None:
    """Renders the top banner and system status indicators."""
    max_y, max_x = stdscr.getmaxyx()
    title = " MTG INVENTORY MANAGEMENT & FULFILLMENT STATION "
    stdscr.attron(curses.color_pair(2) | curses.A_BOLD)
    stdscr.addstr(0, 0, title.center(max_x)[:max_x])
    stdscr.attroff(curses.color_pair(2) | curses.A_BOLD)

    # Connection and stock metrics
    api_status = "Connected (Live API)" if manapool_client.is_configured() else "Offline (CSV Mode)"
    db_name = db.DB_NAME
    
    try:
        summary = db.get_inventory_summary()
        total_cards = sum(s["total_cards"] for s in summary)
        boxes = len({s["box_label"] for s in summary})
        sections = len(summary)
        stats_str = f"Stock: {total_cards:,} cards across {boxes} boxes ({sections} sections)"
    except Exception:
        stats_str = "Stock: Database query error"

    status_line = f" DB: {db_name} | ManaPool: {api_status} | {stats_str}"
    stdscr.addstr(1, 0, status_line[: max_x - 1], curses.color_pair(4))

    # Bottom status bar
    stdscr.addstr(max_y - 1, 0, f" Status: {status_msg}"[: max_x - 1], curses.color_pair(3))


def show_paged_message(stdscr, title: str, lines: List[str]) -> None:
    """Shows a long list in message boxes one screenful at a time."""
    per_page = max(1, stdscr.getmaxyx()[0] - 10)
    pages = [lines[i:i + per_page] for i in range(0, len(lines), per_page)] or [[]]
    for n, page in enumerate(pages, start=1):
        show_message_box(stdscr, f"{title} ({n}/{len(pages)})" if len(pages) > 1 else title, page)


def action_ingest_batch(stdscr, csv_path: Optional[str] = None) -> None:
    """Interactive wizard to ingest a ManaPool set CSV batch (an intake sheet, or any catalog CSV)."""
    max_y, max_x = stdscr.getmaxyx()
    stdscr.clear()
    draw_header(stdscr, "Ingest ManaPool CSV Batch")
    stdscr.addstr(3, 2, "=== INGEST NEW BATCH FROM CSV ===", curses.A_BOLD)

    if csv_path is None:
        # Offer the sheets waiting in intake/, newest first
        pending = intake_sheet.pending_sheets()
        stdscr.addstr(5, 2, "Sheets waiting in intake/:" if pending else "No sheets waiting in intake/.", curses.color_pair(3))
        for n, p in enumerate(pending[: max(0, max_y - 12)], start=1):
            s = intake_sheet.summarize(intake_sheet.read_sheet(p))
            stdscr.addstr(5 + n, 4, f"{os.path.relpath(p)}  ({s['lines']} lines, {s['cards']} cards, "
                                    f"${s['value_cents'] / 100:.2f})"[: max_x - 6])
        stdscr.refresh()
        csv_path = prompt_input(stdscr, "Enter path to ManaPool CSV", os.path.relpath(pending[0]) if pending else "")
    if not os.path.isfile(csv_path):
        show_message_box(stdscr, "Error", [f"File not found: {csv_path}"])
        return

    # Optional section override
    sec_override = prompt_input(stdscr, "Enter Section Override (leave empty for auto Sec-X)", "")
    actual_sec = sec_override if sec_override else None

    # Optional API push
    push_api = False
    if manapool_client.is_configured():
        api_choice = prompt_input(stdscr, "Push directly to ManaPool API? (y/n)", "y").lower()
        push_api = (api_choice == "y")

    # Ingestion progress notice
    show_message_box(stdscr, "Processing", [
        f"Ingesting: {csv_path}",
        "Auto-resolving box locations and section numbers per set...",
        "Resolving cards against the Scryfall mirror & sorting sections...",
        "Please wait a moment...",
    ], wait_key=False)

    def curses_prompt_box(set_code: str) -> str:
        return prompt_input(stdscr, f"New set '{set_code}' detected! Enter box label (e.g. M1..M19)", "M1")

    try:
        res = csv_pipeline.ingest_csv_batch(
            file_path=csv_path,
            section_label=actual_sec,
            push_to_api=push_api,
            prompt_callback=curses_prompt_box,
        )

        if res.get("status") == "success":
            msg_lines = [
                f"Successfully ingested {res['total_cards']} cards across {res['total_lines']} titles!",
                f"Sets: {', '.join(res['sets'])}",
                "",
                "Set Box Allocations:",
            ]
            for s_code, b_lbl in res.get("set_boxes", {}).items():
                msg_lines.append(f"  * Set {s_code} -> Box {b_lbl} / {res['set_sections'][s_code]}")
            if res["skipped_non_singles"]:
                msg_lines.append(f"Skipped {res['skipped_non_singles']} non-single rows.")
            msg_lines.append("")
            msg_lines.append("Generated ManaPool Upload CSVs:")
            for c in res["upload_csvs"]:
                msg_lines.append(f"  {os.path.basename(c)}")

            if push_api:
                msg_lines.append("")
                msg_lines.append(f"ManaPool API: added {res['api_added_copies']} copies to {res['api_updated']} listings.")
                if res["api_mismatches"]:
                    msg_lines.append(f"WARNING: {len(res['api_mismatches'])} listings did not read back as sent.")
                for err in res["api_errors"][:3]:
                    msg_lines.append(f"Error: {err[:100]}")
            if res["retired_to"]:
                msg_lines.append("")
                msg_lines.append(f"Sheet moved to {os.path.relpath(res['retired_to'])}")

            show_message_box(stdscr, "Ingestion Complete", msg_lines)
        else:
            lines = [res.get("message", "No cards ingested.")]
            for r in res.get("unresolved", []):
                lines.append(f"  {r.get('set')} {r.get('number')} {r.get('name')}")
            show_message_box(stdscr, "Ingestion Notice", lines)

    except Exception as e:
        show_message_box(stdscr, "Ingestion Error", [f"An error occurred: {e}"])


RARITY_CODES = {"common": "C", "uncommon": "U", "rare": "R", "mythic": "M", "special": "S", "bonus": "B"}


def count_sheet(stdscr, path: str) -> List[Dict[str, str]]:
    """Count screen: walk an intake sheet in shelf order and enter quantities.

    Digits type a quantity for the highlighted card (Enter or Up/Down commits it),
    +/- or Right/Left adjust by one, / finds a card by name or collector number, Q finishes.
    Every change is saved to the sheet immediately.

    Returns:
        List[Dict[str, str]]: The sheet rows as saved.
    """
    rows = intake_sheet.read_sheet(path)
    if not rows:
        return rows
    mirror = scryfall_mirror.lookup_cards((r["set"], r["number"]) for r in rows)
    rarity = [RARITY_CODES.get((mirror.get((r["set"].upper(), r["number"].lower())) or {}).get("rarity"), "?") for r in rows]
    finish = "foil" if rows[0]["finish"] == "FO" else "non-foil"
    title = f"=== COUNT {rows[0]['set']} ({finish}) : {os.path.basename(path)} ==="
    sel = top = 0
    buf = ""
    last_query = ""

    def qty(i: int) -> int:
        q = (rows[i].get("quantity") or "").strip()
        return int(q) if q.isdigit() else 0

    def set_qty(i: int, q: int) -> None:
        rows[i]["quantity"] = str(max(0, q))
        intake_sheet.write_sheet(path, rows)

    def commit() -> None:
        nonlocal buf
        if buf:
            set_qty(sel, int(buf))
            buf = ""

    while True:
        max_y, max_x = stdscr.getmaxyx()
        list_h = max(1, max_y - 10)
        top = max(min(top, sel), sel - list_h + 1)
        stdscr.erase()
        draw_header(stdscr, "Count Intake Sheet")
        stdscr.addstr(3, 2, title[: max_x - 4], curses.A_BOLD)
        name_w = max(12, min(40, max_x - 52))
        header = f"{'Qty':>5}  {'Name':<{name_w}} {'No.':<6} R {'Price':>7} {'Low':>7} {'Mkt':>7}  Note"
        stdscr.addstr(5, 2, header[: max_x - 4], curses.color_pair(3) | curses.A_BOLD)
        for line in range(min(list_h, len(rows) - top)):
            i = top + line
            r = rows[i]
            market = r["market_price_foil"] if r["finish"] == "FO" else r["market_price"]
            note = "no data" if not r["market_low"] and not market else ""
            q = f"{buf}_" if i == sel and buf else str(qty(i))
            text = (f"{q:>5}  {r['name'][:name_w]:<{name_w}} {r['number'][:6]:<6} {rarity[i]} "
                    f"{r['price'] or '-':>7} {r['market_low'] or '-':>7} {market or '-':>7}  {note}")
            if i == sel:
                attr = curses.A_REVERSE | curses.A_BOLD
            elif qty(i) > 0:
                attr = curses.color_pair(4) | curses.A_BOLD
            else:
                attr = curses.color_pair(1)
            stdscr.addstr(6 + line, 2, text[: max_x - 4], attr)
        s = intake_sheet.summarize(rows)
        totals = f"Counted: {s['lines']} lines | {s['cards']} cards | ${s['value_cents'] / 100:.2f}    (card {sel + 1} of {len(rows)})"
        stdscr.addstr(max_y - 3, 2, totals[: max_x - 4], curses.color_pair(4) | curses.A_BOLD)
        keys = "[0-9] Qty  [Enter] Next  [+/- or Right/Left] Adjust  [/] Find  [Up/Down/PgUp/PgDn] Move  [Q] Done"
        stdscr.addstr(max_y - 2, 2, keys[: max_x - 4], curses.color_pair(1) | curses.A_DIM)
        stdscr.refresh()

        ch = stdscr.getch()
        if ord("0") <= ch <= ord("9"):
            if len(buf) < 4:
                buf += chr(ch)
        elif ch in (curses.KEY_BACKSPACE, 127, 8):
            buf = buf[:-1]
        elif ch in (10, 13, curses.KEY_ENTER, curses.KEY_DOWN):
            commit()
            sel = min(sel + 1, len(rows) - 1)
        elif ch == curses.KEY_UP:
            commit()
            sel = max(sel - 1, 0)
        elif ch == curses.KEY_NPAGE:
            commit()
            sel = min(sel + list_h, len(rows) - 1)
        elif ch == curses.KEY_PPAGE:
            commit()
            sel = max(sel - list_h, 0)
        elif ch == curses.KEY_HOME:
            commit()
            sel = 0
        elif ch == curses.KEY_END:
            commit()
            sel = len(rows) - 1
        elif ch in (ord("+"), ord("="), curses.KEY_RIGHT):
            commit()
            set_qty(sel, qty(sel) + 1)
        elif ch in (ord("-"), curses.KEY_LEFT):
            commit()
            set_qty(sel, qty(sel) - 1)
        elif ch == ord("/"):
            commit()
            query = prompt_input(stdscr, "Find card name or collector number", last_query)
            if query:
                last_query = query
                hit = intake_sheet.find_row(rows, query, sel)
                if hit is None:
                    show_message_box(stdscr, "Not Found", [f"No card matches '{query}'."])
                else:
                    sel = hit
        elif ch == 27:  # Esc: drop a half-typed quantity, or leave
            if buf:
                buf = ""
            else:
                break
        elif ch in (ord("q"), ord("Q")):
            commit()
            break
    return rows


def action_intake_sheet(stdscr) -> None:
    """Builds (or resumes) an intake sheet for one set, counts it, and offers to ingest it."""
    stdscr.clear()
    draw_header(stdscr, "New Intake Sheet")
    stdscr.addstr(3, 2, "=== NEW INTAKE SHEET (COUNT A SET) ===", curses.A_BOLD)
    stdscr.addstr(4, 2, "One set per sheet, NM only. Tokens are handled separately.")
    set_code = prompt_input(stdscr, "Set code (e.g. SOI)", "").upper()
    if not set_code:
        return
    foil = prompt_input(stdscr, "Foils? (y/n)", "n").lower() == "y"

    path = None
    existing = intake_sheet.find_unfinished(set_code, foil)
    if existing and prompt_input(stdscr, f"Resume unfinished {os.path.basename(existing)}? (y/n)", "y").lower() == "y":
        path = existing
        if intake_sheet.is_stale(path):
            show_message_box(stdscr, "Refreshing Prices", ["Prices on this sheet are over a day old; refreshing..."], wait_key=False)
            try:
                res = intake_sheet.refresh_prices(path)
                show_message_box(stdscr, "Prices Refreshed", [f"Updated {res['changed']} prices; kept {res['kept']} edited by hand."])
            except Exception as e:
                show_message_box(stdscr, "Refresh Failed", [f"Could not refresh prices: {e}", "Counting with the old prices."])

    if path is None:
        show_message_box(stdscr, "Building Sheet", [f"Looking up {set_code} in the mirror and on ManaPool..."], wait_key=False)
        try:
            res = intake_sheet.create_sheet(set_code, foil=foil)
        except Exception as e:
            show_message_box(stdscr, "Sheet Error", [f"Error: {e}"])
            return
        missing = [f"  {m['number']:<6} {m['name'][:40]}  ({m['reason']})" for m in res.get("missing", [])]
        if res["status"] != "created":
            show_paged_message(stdscr, "No Sheet", [res["message"]] + missing)
            return
        path = res["path"]
        lines = [f"{len(res['rows'])} cards on {os.path.relpath(path)}"]
        if res["tokens"]:
            lines.append(f"Left out {res['tokens']} token/emblem cards.")
        if res["no_data"]:
            lines.append(f"{res['no_data']} cards have no price data and start at the floor.")
        if missing:
            lines += ["", f"{len(missing)} cards in the mirror are NOT on the sheet:"] + missing
        show_paged_message(stdscr, "Sheet Ready", lines)

    rows = count_sheet(stdscr, path)
    s = intake_sheet.summarize(rows)
    stdscr.clear()
    draw_header(stdscr, "New Intake Sheet")
    if not s["lines"]:
        if prompt_input(stdscr, "Nothing counted. Discard this sheet? (y/n)", "y").lower() == "y":
            intake_sheet.discard_sheet(path)
        return
    show_message_box(stdscr, "Sheet Saved", [os.path.relpath(path),
                                             f"{s['lines']} lines, {s['cards']} cards, ${s['value_cents'] / 100:.2f}"], wait_key=False)
    if prompt_input(stdscr, "Ingest now? (y/n)", "n").lower() == "y":
        action_ingest_batch(stdscr, csv_path=path)


def action_relocate_box(stdscr) -> None:
    """The Box Shuffle function: moves every section of a set to another box when a box runs out of room."""
    max_x = stdscr.getmaxyx()[1]
    stdscr.clear()
    draw_header(stdscr, "Relocate Set / Box Shuffle")
    stdscr.addstr(3, 2, "=== RELOCATE SET (BOX SHUFFLE) ===", curses.A_BOLD)
    stdscr.addstr(4, 2, "Moves every section of a set to another box. Section labels and index numbers stay the same."[: max_x - 4])

    boxes = db.get_distinct_boxes()
    if not boxes:
        show_message_box(stdscr, "Notice", ["No inventory found in database to relocate."])
        return
    stdscr.addstr(6, 2, f"Boxes in use: {', '.join(boxes)}"[: max_x - 4], curses.color_pair(3))
    stdscr.refresh()

    set_code = prompt_input(stdscr, "Set code to move (e.g. BLC, or TOKENS for every token chunk)").upper()
    if not set_code:
        return
    if not db.get_shelf_set_sections(set_code):
        show_message_box(stdscr, "Error", [f"Set {set_code} is not in inventory."])
        return

    target_box = prompt_input(stdscr, f"Box to move {set_code} to (e.g. M20)")
    if not target_box:
        return

    try:
        plan = csv_pipeline.plan_relocation(set_code, target_box)
        if plan["status"] != "ready":
            clashes = [f"  {sec} is in {', '.join(b)}" for sec, b in sorted(plan.get("clashes", {}).items())]
            show_message_box(stdscr, "Nothing Moved", [plan["message"]] + clashes)
            return

        show_message_box(stdscr, "Confirm Relocation", csv_pipeline.format_relocation_plan(plan), wait_key=False)
        if prompt_input(stdscr, f"Move {set_code} to {plan['target_box']}? (y/n)", "n").lower() != "y":
            return

        res = csv_pipeline.relocate_box(set_code, plan["target_box"])
        if res["status"] == "success":
            show_message_box(stdscr, "Relocation Complete",
                             [f"Moved {res['updated_rows']} rows of {set_code} to {res['target_box']}."])
        else:
            show_message_box(stdscr, "Nothing Moved", [res["message"]])
    except Exception as e:
        show_message_box(stdscr, "Error", [f"Relocation failed: {e}"])


def action_run_picklist(stdscr, use_api: bool = False) -> None:
    """Builds the optimized pick sheet, shows its orders, then optionally deducts exactly those picks.

    Live API orders already deducted (in the picked_orders ledger) are left off the sheet and
    listed; each order is deducted at most once. Picklist CSVs are not ledgered.
    """
    stdscr.clear()
    mode_label = "ManaPool Live API" if use_api else "Local Picklist CSV"
    draw_header(stdscr, f"Order Fulfillment: {mode_label}")
    stdscr.addstr(3, 2, f"=== RUN PICKLIST OPTIMIZER ({mode_label.upper()}) ===", curses.A_BOLD)

    csv_path = None
    if not use_api:
        # Check for picklist CSVs
        picks = [f for f in os.listdir(".") if "picklist" in f and f.endswith(".csv")]
        default_pick = picks[0] if picks else ""
        csv_path = prompt_input(stdscr, "Path to Picklist CSV", default_pick)
        if not os.path.isfile(csv_path):
            show_message_box(stdscr, "Error", [f"Picklist file not found: {csv_path}"])
            return

    show_message_box(stdscr, "Optimizing Traversal", [
        "Fetching orders waiting to ship..." if use_api else "Reading picklist...",
        "Resolving order lines against PostgreSQL inventory...",
        "Sorting pick sequence: Box -> Set -> Section -> Index (tokens: Box -> Section -> Index)...",
    ], wait_key=False)

    try:
        res = pull_list_optimizer.process_pick_routine(
            source_type="api" if use_api else "csv",
            csv_file=csv_path,
            commit=False,
        )
    except Exception as e:
        show_message_box(stdscr, "Pick Error", [f"Error processing picklist: {e}"])
        return

    picked_lines = [f"  {o['order_label']} (deducted {o['picked_at']:%Y-%m-%d %H:%M})" for o in res.get("already_picked", [])]
    if picked_lines:
        picked_lines.insert(0, "Already picked and deducted, left off this sheet (ship, then mark shipped):")
    note_lines = [f"[!] {n}" for n in res.get("notes", [])]

    if res.get("status") != "success":
        show_paged_message(stdscr, "Pick Notice", [res.get("message", "No orders to fulfill.")] + picked_lines + note_lines)
        return

    lines = [
        f"Total Cards to Pull: {res['total_cards']} across {res['total_allocated']} pick steps.",
        f"Printable Pick Sheet: {res['sheet_path']}",
    ]
    if res["orders"]:
        lines.append("Orders on this sheet:")
        lines += [f"  {o['order_label']}: {o['cards']} cards" + (f", {o['missing']} MISSING" if o["missing"] else "")
                  for o in res["orders"]]
    if res["unallocated_count"] > 0:
        lines.append(f"WARNING: {res['unallocated_count']} items were NOT in stock or partially missing!")
    lines += picked_lines + note_lines
    show_paged_message(stdscr, "Pick Sheet Ready", lines)

    if not res["allocated_items"] or (use_api and not res["ledgered"]):
        return
    if prompt_input(stdscr, "Pulled these cards? Deduct them from inventory now (once per order)? (y/n)", "n").lower() != "y":
        show_message_box(stdscr, "Not Deducted", ["Inventory unchanged. Pull the cards, then run this again to deduct."])
        return
    labels = None
    if len(res["orders"]) > 1:
        only = prompt_input(stdscr, "Only these order labels, comma-separated (empty = all shown)", "")
        labels = [x.strip() for x in only.split(",") if x.strip()] or None

    try:
        res = pull_list_optimizer.commit_sheet(res, labels)
    except Exception as e:
        show_message_box(stdscr, "Deduction Error", [f"Nothing deducted: {e}"])
        return
    lines = [f"Inventory Deducted: {res['committed_count']} records updated in DB."]
    if res["orders_deducted"]:
        lines.append(f"Recorded as picked: {', '.join(res['orders_deducted'])}")
    if res["orders_skipped"]:
        lines.append(f"Skipped, already picked: {', '.join(res['orders_skipped'])}")
    if res["source"] == "csv":
        lines.append("[!] Picklist CSV: not recorded in the order ledger.")
    lines.append("Next: ship, mark shipped on ManaPool, then check: python -m station.audit push (dry run).")
    show_message_box(stdscr, "Deducted", lines)


def action_browse_inventory(stdscr) -> None:
    """Displays a scrollable view of all physical inventory sections."""
    stdscr.clear()
    draw_header(stdscr, "Browse Physical Inventory")
    max_y, max_x = stdscr.getmaxyx()

    summary = db.get_inventory_summary()
    if not summary:
        show_message_box(stdscr, "Empty Inventory", ["No cards currently cataloged in physical inventory."])
        return

    stdscr.addstr(3, 2, "=== PHYSICAL STORAGE BREAKDOWN ===", curses.A_BOLD)
    header_cols = f"{'Box':<12} {'Set':<8} {'Section':<10} {'Unique Titles':<16} {'Total Cards':<14} {'Index Range':<12}"
    stdscr.addstr(5, 2, header_cols[: max_x - 4], curses.color_pair(3) | curses.A_BOLD)
    stdscr.addstr(6, 2, ("-" * (len(header_cols) + 4))[: max_x - 4])

    scroll_pos = 0
    visible_rows = max(1, max_y - 10)

    while True:
        visible_rows = max(1, max_y - 10)
        for idx in range(visible_rows):
            item_idx = scroll_pos + idx
            row_y = 7 + idx
            if row_y >= max_y - 3:
                break
            stdscr.move(row_y, 2)
            stdscr.clrtoeol()
            if item_idx < len(summary):
                s = summary[item_idx]
                idx_range = f"{s['min_index']} - {s['max_index']}" if s["min_index"] else "N/A"
                row_str = f"{s['box_label']:<12} {s['set_code'].upper():<8} {s['section_label']:<10} {s['total_lines']:<16} {s['total_cards']:<14} {idx_range:<12}"
                stdscr.addstr(row_y, 2, row_str[: max_x - 4])

        prompt_str = f"[ Row {scroll_pos+1} to {min(scroll_pos+visible_rows, len(summary))} of {len(summary)} | UP/DOWN/PgUp/PgDn to Scroll | Q to Return ]"
        stdscr.addstr(max_y - 2, max(2, (max_x - len(prompt_str)) // 2), prompt_str[: max_x - 4], curses.color_pair(4) | curses.A_BOLD)
        stdscr.refresh()

        ch = stdscr.getch()
        if ch in [ord("q"), ord("Q"), 27]:
            break
        elif ch == curses.KEY_UP and scroll_pos > 0:
            scroll_pos -= 1
        elif ch == curses.KEY_DOWN and scroll_pos + visible_rows < len(summary):
            scroll_pos += 1
        elif ch == curses.KEY_PPAGE:  # Page Up
            scroll_pos = max(0, scroll_pos - visible_rows)
        elif ch == curses.KEY_NPAGE:  # Page Down
            scroll_pos = min(max(0, len(summary) - visible_rows), scroll_pos + visible_rows)
        elif ch == curses.KEY_RESIZE:
            max_y, max_x = stdscr.getmaxyx()
            stdscr.clear()
            draw_header(stdscr, "Browse Physical Inventory")
            stdscr.addstr(3, 2, "=== PHYSICAL STORAGE BREAKDOWN ===", curses.A_BOLD)
            stdscr.addstr(5, 2, header_cols[: max_x - 4], curses.color_pair(3) | curses.A_BOLD)
            stdscr.addstr(6, 2, ("-" * (len(header_cols) + 4))[: max_x - 4])


def action_diagnostics(stdscr) -> None:
    """Runs diagnostics on PostgreSQL database and ManaPool API connections."""
    stdscr.clear()
    draw_header(stdscr, "Diagnostics & Configuration")
    stdscr.addstr(3, 2, "=== SYSTEM DIAGNOSTICS ===", curses.A_BOLD)

    lines = []
    # Test Database
    try:
        conn = db.get_connection()
        with conn.cursor() as cur:
            cur.execute("SELECT version();")
            pg_ver = cur.fetchone()[0]
        conn.close()
        lines.append(f"PostgreSQL: Connected ({pg_ver.split(',')[0]})")
        lines.append(f"  Host: {db.DB_HOST}:{db.DB_PORT}  | Database: {db.DB_NAME} | User: {db.DB_USER}")
    except Exception as e:
        lines.append(f"PostgreSQL: Connection FAILED - {e}")

    lines.append("")
    # Test ManaPool API
    if manapool_client.is_configured():
        ok, msg = manapool_client.test_connection()
        status_txt = "Connected" if ok else "Failed"
        lines.append(f"ManaPool API: {status_txt} ({msg})")
        lines.append(f"  Email: {manapool_client.EMAIL}")
    else:
        lines.append("ManaPool API: Not configured in .env (Offline / CSV Mode)")
        lines.append("  To enable live API integration, add to .env:")
        lines.append("  MANAPOOL_EMAIL=your_email@example.com")
        lines.append("  MANAPOOL_API_TOKEN=mpat_your_token_here")

    show_message_box(stdscr, "Diagnostics", lines)


def action_refresh_mirror(stdscr) -> None:
    """Downloads Scryfall's Default Cards bulk file into the local scryfall_cards mirror."""
    stdscr.clear()
    draw_header(stdscr, "Refreshing Scryfall Mirror")
    show_message_box(stdscr, "Scryfall Mirror", ["Downloading and loading Default Cards bulk file...", "This takes a few minutes."], wait_key=False)
    try:
        result = scryfall_mirror.refresh_mirror()
        status = scryfall_mirror.get_mirror_status()
        lines = [
            f"Loaded {result['cards_written']:,} cards (Scryfall bulk updated {result['bulk_updated_at'] or 'n/a'}).",
            f"Mirror now holds {status['card_count']:,} cards.",
        ]
    except Exception as e:
        lines = [f"Refresh FAILED: {e}"]
    show_message_box(stdscr, "Scryfall Mirror", lines)


def action_personal_pull(stdscr) -> None:
    """Search inventory or load a decklist, pull cards for a personal collection, and sync ManaPool."""
    stdscr.clear()
    draw_header(stdscr, "Personal Collection Pull")
    stdscr.addstr(3, 2, "=== PULL CARDS FOR A PERSONAL COLLECTION / DECK ===", curses.A_BOLD)
    stdscr.addstr(4, 2, "Enter a card name, part of a name to search, or a path to a decklist file.")

    query = prompt_input(stdscr, "Card name / search text / decklist path", "")
    if not query:
        return

    if os.path.isfile(query):
        with open(query, encoding="utf-8") as f:
            requests_, unparsed = collection_pull.parse_decklist(f)
        label = os.path.basename(query)
        if unparsed:
            show_message_box(stdscr, "Skipped Lines", [f"Could not parse {len(unparsed)} lines:"] + unparsed[:10])
    else:
        hits = collection_pull.search_stock(collection_pull.load_stock(), query)
        if not hits:
            show_message_box(stdscr, "No Match", [f"No in-stock cards match '{query}'."])
            return
        lines = []
        for n, r in enumerate(hits[:15], start=1):
            price = f"${float(r['manapool_listed_price']):.2f}" if r["manapool_listed_price"] is not None else "-"
            finish = "FOIL" if r["is_foil"] else "NF"
            lines.append(f"{n:>2}. {r['name'][:30]:<30} {r['set_code']:<5} {r['collector_number']:<6} {finish:<4} x{r['quantity']:<3} {price:<7} {r['box_label']} {r['section_label']} #{r['index_num']:03d}")
        if len(hits) > 15:
            lines.append(f"... {len(hits) - 15} more; refine the search to see them.")
        show_message_box(stdscr, f"{len(hits)} In-Stock Matches", lines)

        choice = prompt_input(stdscr, "Row number to pull (empty = cancel)", "1")
        if not choice.isdigit() or not 1 <= int(choice) <= min(len(hits), 15):
            return
        row = hits[int(choice) - 1]
        qty_str = prompt_input(stdscr, f"Copies of {row['name']} to pull (max {row['quantity']})", "1")
        if not qty_str.isdigit() or int(qty_str) < 1:
            return
        requests_ = [{"qty": int(qty_str), "name": row["name"], "set_code": row["set_code"],
                      "inventory_id": row["inventory_id"], "foil": row["is_foil"]}]
        label = f"{qty_str} x {row['name']} ({row['set_code']} {row['collector_number']})"

    def confirm(summary: List[str]) -> bool:
        show_message_box(stdscr, "Confirm Pull", summary, wait_key=False)
        return prompt_input(stdscr, "Deduct from inventory and sync ManaPool? (y/n)", "n").lower() == "y"

    show_message_box(stdscr, "Planning", ["Checking open orders and choosing copies..."], wait_key=False)
    try:
        res = collection_pull.run_pull(requests_, label, confirm=confirm)
        if res["blocked"]:
            show_message_box(stdscr, "Open Orders Not Checked", [res["order_status"]], wait_key=False)
            if prompt_input(stdscr, "Pull anyway, without holding open-order copies? (y/n)", "n").lower() != "y":
                return
            res = collection_pull.run_pull(requests_, label, confirm=confirm, force=True)
    except Exception as e:
        show_message_box(stdscr, "Pull Error", [f"Error: {e}"])
        return

    lines = [f"Pull sheet: {res['sheet_path']}", res["order_status"]]
    for s in res["shortfalls"][:8]:
        why = "held for open order" if s["held_for_orders"] else ("not enough in stock" if s["found_any"] else "not in stock")
        lines.append(f"Short: {s['request']['name'][:30]} missing {s['missing_qty']} ({why})")
    if res["committed"]:
        lines.append(f"Deducted {sum(p['qty_to_pull'] for p in res['picks'])} cards from inventory.")
        lines += collection_pull.sync_lines(res["sync"])
    elif res["picks"]:
        lines.append("Not confirmed: inventory unchanged.")
    else:
        lines.append("Nothing available to pull.")
    show_message_box(stdscr, "Personal Pull", lines)


def action_reprice(stdscr) -> None:
    """Runs the repricer: drift check, plan, review over-limit changes, confirm, push, verify."""
    stdscr.clear()
    draw_header(stdscr, "Reprice Listings")
    stdscr.addstr(3, 2, "=== REPRICE FROM MANAPOOL MARKET DATA ===", curses.A_BOLD)
    preview_only = prompt_input(stdscr, "Preview only, push nothing? (y/n)", "y").lower() == "y"

    def review(items: List[Dict[str, Any]]) -> Dict[str, Optional[int]]:
        lines = [f"{n:>2}. {i['name'][:28]:<28} {i['set_code']:<5} {'FOIL' if i['is_foil'] else 'NF':<4} "
                 f"{repricer._fmt(i['listed_cents']):>7} -> {repricer._fmt(i['target_cents']):<7} "
                 f"low {repricer._fmt(i['low'])} mkt {repricer._fmt(i['market'])} {' '.join(i['signals'])}"
                 for n, i in enumerate(items[:20], start=1)]
        if len(items) > 20:
            lines.append(f"... {len(items) - 20} more (see the report CSV)")
        show_message_box(stdscr, f"{len(items)} Changes Need Review", lines)
        choice = prompt_input(stdscr, "[a]ccept all, [r]eject all, [o]ne by one", "r").lower()
        if choice == "a":
            return {i["product_id"]: i["target_cents"] for i in items}
        if choice != "o":
            return {i["product_id"]: None for i in items}
        floor = repricer._cents(repricer.load_config()["pricing"]["floor"])
        decisions: Dict[str, Optional[int]] = {}
        for i in items:
            ans = prompt_input(
                stdscr, f"{i['name'][:30]} {repricer._fmt(i['listed_cents'])} -> {repricer._fmt(i['target_cents'])}: "
                "[a]ccept, [s]kip, or a price", "s").lower()
            if ans == "a":
                decisions[i["product_id"]] = i["target_cents"]
            elif ans.replace(".", "", 1).isdigit():
                decisions[i["product_id"]] = max(repricer._cents(ans), floor)
            else:
                decisions[i["product_id"]] = None
        return decisions

    def confirm(lines: List[str]) -> bool:
        show_message_box(stdscr, "Confirm Reprice", lines, wait_key=False)
        return prompt_input(stdscr, "Push these price changes to ManaPool? (y/n)", "n").lower() == "y"

    show_message_box(stdscr, "Repricing", ["Checking live prices, downloading the ManaPool price feed...",
                                           "This takes about a minute."], wait_key=False)
    try:
        res = repricer.run_reprice(review=review, confirm=confirm, preview=preview_only)
    except Exception as e:
        show_message_box(stdscr, "Reprice Error", [f"Error: {e}"])
        return
    if res["blocked"]:
        show_message_box(stdscr, "Reprice Blocked", [res["blocked"]])
        return
    lines = list(res["header"]) + ["", f"Report: {res['txt_path']}"]
    if res["pushed"] or res["errors"]:
        lines.append(f"Pushed {res['pushed']} prices; verified {len(res['applied'])}; {len(res['mismatches'])} mismatches.")
        lines.extend(f"Error: {e[:80]}" for e in res["errors"][:3])
    show_message_box(stdscr, "Reprice Result", lines)


def action_price_pins(stdscr) -> None:
    """Lists, adds and removes repricer pins."""
    while True:
        stdscr.clear()
        draw_header(stdscr, "Repricer Price Pins")
        pins = repricer.list_pins()
        lines = [f"{n:>2}. {(p['name'] or '?')[:28]:<28} {p['set_code'] or '':<5} {p['collector_number'] or '':<6} "
                 f"{'FOIL' if p['is_foil'] else 'NF':<4} listed {p['listed_price']} "
                 f"{'fixed $' + str(p['fixed_price']) if p['fixed_price'] is not None else '(hold)'} {p['note'] or ''}"
                 for n, p in enumerate(pins, start=1)] or ["No pins."]
        show_message_box(stdscr, f"{len(pins)} Pinned Products", lines)
        action = prompt_input(stdscr, "[a]dd pin, [r]emove pin, or [q]uit", "q").lower()
        if action == "a":
            set_code = prompt_input(stdscr, "Set code", "").upper()
            number = prompt_input(stdscr, "Collector number", "")
            foil = prompt_input(stdscr, "Foil? (y/n)", "n").lower() == "y"
            ids = repricer.find_product_ids(set_code, number, foil)
            if len(ids) != 1:
                show_message_box(stdscr, "Not Found", [f"{len(ids)} products match {set_code} {number} {'FOIL' if foil else 'NF'}."])
                continue
            price = prompt_input(stdscr, "Fixed price in dollars (empty = hold listed price)", "")
            note = prompt_input(stdscr, "Note (optional)", "")
            try:
                repricer.add_pin(ids[0], float(price) if price else None, note or None)
            except ValueError as e:
                show_message_box(stdscr, "Pin Error", [str(e)])
        elif action == "r" and pins:
            n = prompt_input(stdscr, "Pin number to remove", "")
            if n.isdigit() and 1 <= int(n) <= len(pins):
                repricer.remove_pin(pins[int(n) - 1]["manapool_product_id"])
        else:
            return


def main_loop(stdscr) -> None:
    """Main Curses event loop."""
    curses.curs_set(0)
    curses.start_color()
    curses.use_default_colors()

    # Color definitions
    curses.init_pair(1, curses.COLOR_WHITE, -1)           # Standard text
    curses.init_pair(2, curses.COLOR_BLACK, curses.COLOR_CYAN)  # Header banner
    curses.init_pair(3, curses.COLOR_YELLOW, -1)          # Prompts / Highlights
    curses.init_pair(4, curses.COLOR_GREEN, -1)           # Success / Status
    curses.init_pair(5, curses.COLOR_RED, -1)             # Errors / Warnings

    curses.set_escdelay(25)  # Esc on the count screen responds without the default 1 s wait

    # (key, label, action); a None action quits
    menu_items = [
        ("N", "New Intake Sheet (count a set, then ingest)", action_intake_sheet),
        ("1", "Ingest ManaPool Set CSV Batch (Sort & Assign Physical Indices)", action_ingest_batch),
        ("2", "Push Inventory Batch to ManaPool Storefront (API Sync)", action_ingest_batch),  # ingest offers the push
        ("3", "Pull Live Orders & Generate Pick Sheet (ManaPool API)", lambda s: action_run_picklist(s, use_api=True)),
        ("4", "Process Local Order Picklist CSV (e.g. picklist-*.csv)", lambda s: action_run_picklist(s, use_api=False)),
        ("5", "Browse Physical Inventory (Boxes, Sets, Sections)", action_browse_inventory),
        ("6", "Relocate Set to Another Box (Box Shuffle when space runs out)", action_relocate_box),
        ("7", "Run System Diagnostics & Connection Check", action_diagnostics),
        ("8", "Refresh Local Scryfall Mirror (Default Cards bulk file)", action_refresh_mirror),
        ("9", "Pull Cards for Personal Collection / Deck (search or decklist)", action_personal_pull),
        ("R", "Reprice Listings from ManaPool Market Data (preview / review / push)", action_reprice),
        ("P", "Manage Repricer Price Pins", action_price_pins),
        ("Q", "Quit", None),
    ]
    by_key = {ord(k): a for k, _, a in menu_items} | {ord(k.lower()): a for k, _, a in menu_items}

    selected_idx = 0

    while True:
        stdscr.clear()
        draw_header(stdscr, "Select an action using Arrow Keys or Keys (N, 1-9, R, P, Q)")

        max_y, max_x = stdscr.getmaxyx()
        start_y = 4

        stdscr.addstr(start_y, 4, "ACTIONS:", curses.color_pair(3) | curses.A_BOLD)

        spacing = 2 if start_y + 2 + len(menu_items) * 2 < max_y - 3 else 1
        for idx, (key, label, _) in enumerate(menu_items):
            y = start_y + 2 + (idx * spacing)
            if y >= max_y - 3:
                break

            if idx == selected_idx:
                stdscr.attron(curses.A_REVERSE | curses.A_BOLD)
                item_str = f"  -> [{key}] {label:<65} "
                stdscr.addstr(y, 4, item_str[: max_x - 8])
                stdscr.attroff(curses.A_REVERSE | curses.A_BOLD)
            else:
                stdscr.addstr(y, 4, f"     [{key}] ", curses.color_pair(3) | curses.A_BOLD)
                stdscr.addstr(y, 12, f"{label}"[: max_x - 14], curses.color_pair(1))

        # Hotkey guide at bottom
        guide = "[ENTER] Select  |  [N/1-9/R/P] Direct Jump  |  [Q] Exit Station"
        stdscr.addstr(max_y - 2, max(2, (max_x - len(guide)) // 2), guide, curses.color_pair(1) | curses.A_DIM)

        stdscr.refresh()
        ch = stdscr.getch()

        if ch == curses.KEY_UP:
            selected_idx = (selected_idx - 1) % len(menu_items)
            continue
        if ch == curses.KEY_DOWN:
            selected_idx = (selected_idx + 1) % len(menu_items)
            continue
        if ch in [10, 13, curses.KEY_ENTER]:
            action = menu_items[selected_idx][2]
        elif ch in by_key:
            action = by_key[ch]
        else:
            continue
        if action is None:
            break
        action(stdscr)


def main():
    """Runs the curses TUI."""
    try:
        curses.wrapper(main_loop)
    except KeyboardInterrupt:
        pass
    print("\nMTG Inventory Management Station exited cleanly.")


if __name__ == "__main__":
    main()
