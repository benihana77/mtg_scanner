#!/usr/bin/env python3
"""Curses-based Terminal User Interface (TUI) for MTG Inventory Management.

Provides an interactive console dashboard for:
1. Ingesting ManaPool Set CSV batches (with alphabetical sorting and index numbering).
2. Pushing inventory updates directly to the ManaPool API (or generating upload CSVs).
3. Pulling live orders from ManaPool and generating optimized pick traversal sheets.
4. Processing local picklist CSV files.
5. Browsing physical inventory breakdown (Boxes, Sets, Sections, Index ranges).
6. Relocating sets, sections or all TOKENS between boxes (Box Shuffle when space runs out).
7. Connection diagnostics.
8. Refreshing the local Scryfall mirror (scryfall_cards).
9. Pulling cards for a personal collection / deck (search, decklist, ManaPool sync).
"""

import curses
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import db
import collection_pull
import csv_pipeline
import manapool_client
import pull_list_optimizer
import scryfall_mirror


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


def action_ingest_batch(stdscr) -> None:
    """Interactive wizard to ingest a ManaPool set CSV batch."""
    max_y, max_x = stdscr.getmaxyx()
    stdscr.clear()
    draw_header(stdscr, "Ingest ManaPool CSV Batch")
    stdscr.addstr(3, 2, "=== INGEST NEW BATCH FROM CSV ===", curses.A_BOLD)

    # Suggest available CSV files in current directory
    csv_candidates = [f for f in os.listdir(".") if f.endswith(".csv") and "picklist" not in f]
    default_csv = csv_candidates[0] if csv_candidates else "multiple_sets_all_products_2026-09-20T00-27-05.csv"

    csv_path = prompt_input(stdscr, "Enter path to ManaPool CSV", default_csv)
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
                msg_lines.append(f"ManaPool API: Pushed {res['api_updated']} items directly.")
                if res["api_errors"]:
                    msg_lines.append(f"Warnings: {len(res['api_errors'])} errors reported.")

            show_message_box(stdscr, "Ingestion Complete", msg_lines)
        else:
            lines = [res.get("message", "No cards ingested.")]
            for r in res.get("unresolved", []):
                lines.append(f"  {r.get('set')} {r.get('number')} {r.get('name')}")
            show_message_box(stdscr, "Ingestion Notice", lines)

    except Exception as e:
        show_message_box(stdscr, "Ingestion Error", [f"An error occurred: {e}"])


def action_relocate_box(stdscr) -> None:
    """The Box Shuffle function: relocates a set or section when a box runs out of room."""
    stdscr.clear()
    draw_header(stdscr, "Relocate Set / Box Shuffle")
    stdscr.addstr(3, 2, "=== RELOCATE SET / SECTION (BOX SHUFFLE) ===", curses.A_BOLD)
    stdscr.addstr(4, 2, "Transfer cards to another box when physical storage capacity is reached.")

    boxes = db.get_distinct_boxes()
    if not boxes:
        show_message_box(stdscr, "Notice", ["No inventory found in database to relocate."])
        return

    # Show existing boxes and contents
    stdscr.addstr(6, 2, "Current Box Contents:", curses.color_pair(3) | curses.A_BOLD)
    row_idx = 7
    for b in boxes:
        sets = db.get_sets_in_box(b)
        set_summary = ", ".join(f"{s['set_code'].upper()} ({s['total_cards']} cards)" for s in sets)
        stdscr.addstr(row_idx, 4, f"* {b:<10}: {set_summary}"[: stdscr.getmaxyx()[1] - 6])
        row_idx += 1

    stdscr.refresh()

    source_box = prompt_input(stdscr, "Source Box to move from", boxes[0])
    if source_box not in boxes:
        show_message_box(stdscr, "Error", [f"Box '{source_box}' does not exist in inventory."])
        return

    sets_in_src = db.get_sets_in_box(source_box)
    if not sets_in_src:
        show_message_box(stdscr, "Error", [f"No sets found in {source_box}."])
        return

    default_set = sets_in_src[0]["set_code"].upper()
    set_code = prompt_input(stdscr, "Set Code to relocate (e.g. ZNR, or TOKENS for all token chunks)", default_set).upper()

    sec_filter = prompt_input(stdscr, "Specific Section to move (leave empty for ALL sections of this set)", "")
    actual_sec = sec_filter if sec_filter else None

    target_box = prompt_input(stdscr, "Destination Box Label (e.g. Box-2 or new box)", "Box-2")

    confirm = prompt_input(stdscr, f"Confirm moving [{set_code}] from {source_box} -> {target_box}? (y/n)", "y")
    if confirm.lower() != "y":
        return

    show_message_box(stdscr, "Relocating", ["Updating database records..."], wait_key=False)

    try:
        res = csv_pipeline.relocate_box(
            source_box=source_box,
            target_box=target_box,
            set_code=set_code,
            section_label=actual_sec,
        )

        if res.get("status") == "success":
            lines = [
                f"Successfully relocated {res['updated_rows']} items to {target_box}!",
                f"Source: {source_box}  -->  Target: {target_box}",
                f"Set: {set_code}",
            ]
            show_message_box(stdscr, "Relocation Complete", lines)
        else:
            show_message_box(stdscr, "Notice", [res.get("message", "Relocation failed.")])

    except Exception as e:
        show_message_box(stdscr, "Error", [f"Relocation failed: {e}"])


def action_run_picklist(stdscr, use_api: bool = False) -> None:
    """Optimizes order picklist traversal route and optionally deducts inventory."""
    stdscr.clear()
    mode_label = "ManaPool Live API" if use_api else "Local Picklist CSV"
    draw_header(stdscr, f"Order Fulfillment: {mode_label}")
    stdscr.addstr(3, 2, f"=== RUN PICKLIST OPTIMIZER ({mode_label.upper()}) ===", curses.A_BOLD)

    csv_path = None
    if not use_api:
        # Check for picklist CSVs
        picks = [f for f in os.listdir(".") if "picklist" in f and f.endswith(".csv")]
        default_pick = picks[0] if picks else "picklist-2026-09-20.csv"
        csv_path = prompt_input(stdscr, "Path to Picklist CSV", default_pick)
        if not os.path.isfile(csv_path):
            show_message_box(stdscr, "Error", [f"Picklist file not found: {csv_path}"])
            return

    # Ask if stock should be deducted
    commit_choice = prompt_input(stdscr, "Deduct pulled cards from inventory? (y/n)", "n").lower()
    commit_flag = (commit_choice == "y")

    show_message_box(stdscr, "Optimizing Traversal", [
        "Resolving order lines against PostgreSQL inventory...",
        "Sorting pick sequence: Box -> Set -> Section -> Index (tokens: Box -> Section -> Index)...",
    ], wait_key=False)

    try:
        res = pull_list_optimizer.process_pick_routine(
            source_type="api" if use_api else "csv",
            csv_file=csv_path,
            commit=commit_flag,
        )

        if res.get("status") == "success":
            lines = [
                f"Optimal pick sequence calculated!",
                f"Total Cards to Pull: {res['total_cards']} across {res['total_allocated']} pick steps.",
                f"Printable Pick Sheet: {os.path.basename(res['sheet_path'])}",
                f"Location: {res['sheet_path']}",
            ]
            if res["unallocated_count"] > 0:
                lines.append("")
                lines.append(f"WARNING: {res['unallocated_count']} items were NOT in stock or partially missing!")

            if res["committed"]:
                lines.append("")
                lines.append(f"Inventory Deducted: {res['committed_count']} records updated in DB.")

            show_message_box(stdscr, "Pick Sheet Ready", lines)
        else:
            show_message_box(stdscr, "Pick Notice", [res.get("message", "No orders to fulfill.")])

    except Exception as e:
        show_message_box(stdscr, "Pick Error", [f"Error processing picklist: {e}"])


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
                      "ids": (row["scryfall_id"], row["back_scryfall_id"]), "foil": row["is_foil"]}]
        label = f"{qty_str} x {row['name']} ({row['set_code']} {row['collector_number']})"

    def confirm(summary: List[str]) -> bool:
        show_message_box(stdscr, "Confirm Pull", summary, wait_key=False)
        return prompt_input(stdscr, "Deduct from inventory and sync ManaPool? (y/n)", "n").lower() == "y"

    show_message_box(stdscr, "Planning", ["Checking open orders and choosing copies..."], wait_key=False)
    try:
        res = collection_pull.run_pull(requests_, label, confirm=confirm)
    except Exception as e:
        show_message_box(stdscr, "Pull Error", [f"Error: {e}"])
        return

    lines = [f"Pull sheet: {res['sheet_path']}", res["order_status"]]
    for s in res["shortfalls"][:8]:
        why = "held for open order" if s["held_for_orders"] else ("not enough in stock" if s["found_any"] else "not in stock")
        lines.append(f"Short: {s['request']['name'][:30]} missing {s['missing_qty']} ({why})")
    if res["committed"]:
        lines.append(f"Deducted {sum(p['qty_to_pull'] for p in res['picks'])} cards from inventory.")
        sync = res["sync"]
        if sync["mode"] == "api":
            lines.append(f"ManaPool: updated {sync['updated']} products.")
        elif sync["mode"] == "csv":
            lines.append("ManaPool NOT updated. Upload absolute quantities by hand:")
            lines.append(f"  {sync['csv_path']}")
    elif res["picks"]:
        lines.append("Not confirmed: inventory unchanged.")
    else:
        lines.append("Nothing available to pull.")
    show_message_box(stdscr, "Personal Pull", lines)


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

    menu_items = [
        ("[1]", "Ingest ManaPool Set CSV Batch (Sort & Assign Physical Indices)"),
        ("[2]", "Push Inventory Batch to ManaPool Storefront (API Sync)"),
        ("[3]", "Pull Live Orders & Generate Pick Sheet (ManaPool API)"),
        ("[4]", "Process Local Order Picklist CSV (e.g. picklist-*.csv)"),
        ("[5]", "Browse Physical Inventory (Boxes, Sets, Sections)"),
        ("[6]", "Relocate Set / Section (Box Shuffle when space runs out)"),
        ("[7]", "Run System Diagnostics & Connection Check"),
        ("[8]", "Refresh Local Scryfall Mirror (Default Cards bulk file)"),
        ("[9]", "Pull Cards for Personal Collection / Deck (search or decklist)"),
        ("[Q]", "Quit"),
    ]

    selected_idx = 0

    while True:
        stdscr.clear()
        draw_header(stdscr, "Select an action using Arrow Keys or Number Keys (1-9, Q)")

        max_y, max_x = stdscr.getmaxyx()
        start_y = 4

        stdscr.addstr(start_y, 4, "ACTIONS:", curses.color_pair(3) | curses.A_BOLD)

        for idx, (shortcut, label) in enumerate(menu_items):
            y = start_y + 2 + (idx * 2)
            if y >= max_y - 3:
                break

            if idx == selected_idx:
                stdscr.attron(curses.A_REVERSE | curses.A_BOLD)
                item_str = f"  -> {shortcut} {label:<65} "
                stdscr.addstr(y, 4, item_str[: max_x - 8])
                stdscr.attroff(curses.A_REVERSE | curses.A_BOLD)
            else:
                stdscr.addstr(y, 4, f"     {shortcut} ", curses.color_pair(3) | curses.A_BOLD)
                stdscr.addstr(y, 12, f"{label}"[: max_x - 14], curses.color_pair(1))

        # Hotkey guide at bottom
        guide = "[ENTER] Select  |  [1-9] Direct Jump  |  [Q] Exit Station"
        stdscr.addstr(max_y - 2, max(2, (max_x - len(guide)) // 2), guide, curses.color_pair(1) | curses.A_DIM)

        stdscr.refresh()
        ch = stdscr.getch()

        if ch in [ord("q"), ord("Q")]:
            break
        elif ch == curses.KEY_UP:
            selected_idx = (selected_idx - 1) % len(menu_items)
        elif ch == curses.KEY_DOWN:
            selected_idx = (selected_idx + 1) % len(menu_items)
        elif ch in [10, 13, curses.KEY_ENTER]:
            # Execute selected item
            choice = selected_idx + 1
            if choice == 1:
                action_ingest_batch(stdscr)
            elif choice == 2:
                action_ingest_batch(stdscr)  # Ingest wizard offers API push
            elif choice == 3:
                action_run_picklist(stdscr, use_api=True)
            elif choice == 4:
                action_run_picklist(stdscr, use_api=False)
            elif choice == 5:
                action_browse_inventory(stdscr)
            elif choice == 6:
                action_relocate_box(stdscr)
            elif choice == 7:
                action_diagnostics(stdscr)
            elif choice == 8:
                action_refresh_mirror(stdscr)
            elif choice == 9:
                action_personal_pull(stdscr)
            elif choice == 10:
                break
        elif ch == ord("1"):
            action_ingest_batch(stdscr)
        elif ch == ord("2"):
            action_ingest_batch(stdscr)
        elif ch == ord("3"):
            action_run_picklist(stdscr, use_api=True)
        elif ch == ord("4"):
            action_run_picklist(stdscr, use_api=False)
        elif ch == ord("5"):
            action_browse_inventory(stdscr)
        elif ch == ord("6"):
            action_relocate_box(stdscr)
        elif ch == ord("7"):
            action_diagnostics(stdscr)
        elif ch == ord("8"):
            action_refresh_mirror(stdscr)
        elif ch == ord("9"):
            action_personal_pull(stdscr)


def main():
    """Runs the curses TUI."""
    try:
        curses.wrapper(main_loop)
    except KeyboardInterrupt:
        pass
    print("\nMTG Inventory Management Station exited cleanly.")


if __name__ == "__main__":
    main()
