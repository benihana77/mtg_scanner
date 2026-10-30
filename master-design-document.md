Master Design Document & Technical Specification (v2)
> **Target Audience:** LLM Coding Agents (Cursor, Claude Code, GitHub Copilot) & Systems Engineers  
> **Project:** Custom MTG Card Scanning & Inventory Management Station  
> **Repository:** `benihana77/mtg_scanner`
---
🤖 Prompt Engineering Directives for LLM Coding Agents
When executing tasks within this repository, all LLM coding agents MUST adhere to the following operational directives:
Role & Mindset: Act as a Senior Systems and Computer Vision Engineer specializing in embedded Linux (Raspberry Pi), real-time OpenCV processing, FastAPI, and PostgreSQL database pipelines.
Hardware-First Constraint Rule: DO NOT attempt to replace physical hardware constraints (e.g., cross-polarization, dark scanning mats, pre-sorted batches) with complex machine learning or auto-grading algorithms. Software design MUST assume fixed lighting and NM (Near Mint) condition hardcoding.
Incremental Execution: Implement features step-by-step according to Section 8 (Implementation Roadmap). Do not refactor unrelated files or modules unless explicitly instructed.
Code Quality Standards:
Write clean, production-grade Python 3.12+ code.
Enforce strict type hints (`typing` module) across all function signatures.
Include clear docstrings (Google style) for all classes, methods, and API routes.
Maintain defensive error handling, specifically for camera feed drops, Scryfall API rate limits, and database connection retries.
Database Integrity: Do not modify table schema definitions or column names without explicit instructions. Strictly preserve the schema specified in Section 6.
---
1. Project Overview & System Philosophy
This project defines a custom, hardware-anchored Magic: The Gathering (MTG) card scanning and inventory listing system designed to streamline bulk card cataloging for a ManaPool storefront.
Core Philosophy
Commercial scanning apps often bottleneck high-volume bulk processing due to foil glare, set misidentification, and reliance on touchscreen interactions. This project replaces software complexity with physical environment controls and targeted scope bounds:
Glare Elimination: Cross-polarized lighting cancels reflections physically rather than in software.
Search Space Reduction: Matching is scoped exclusively to pre-fetched image hashes of the 2–3 active sets being scanned.
Human-in-the-Loop Velocity: A physical USB numpad allows instant quantity entry and visual confirmation without taking hands off the card stack.
Physical Fulfillment Efficiency: Location-aware inventory indexing enables optimized, sequential order pull lists that eliminate physical warehouse backtracking.
Operational Constraints & Assumptions
Condition: All scanned cards are assumed to be Near Mint (NM). Automated condition grading is explicitly out of scope.
Batching: Cards are pre-sorted into alphabetized stacks of ~1,000 cards across 2 to 3 target sets.
Identical Stacks: The workflow handles consecutive identical cards (e.g., 10+ copies of the same card) via manual quantity selection on a numpad, avoiding frame-differencing failure modes.
---
2. Hardware Architecture & Rig Specifications
Component	Specification	Technical Function
Compute Node	Raspberry Pi 4 (or Pi 3), 64-bit OS	Hosts OpenCV engine, FastAPI server, and numpad listener
Camera Module	Raspberry Pi Camera Module 3 Standard (12MP Sony IMX708, I2C autofocus actuator, 75° FoV) or 1080p USB Webcam	Mounted downward on a copy stand 8–12 inches overhead
Illumination	Microscope LED Ring Light	Surrounds the camera lens to provide direct, uniform lighting
Optics / Polarization	Selens Linear Polarizing Sheet	Two polarizing sheets placed 90° cross-rotated (one over lens, one over ring light) to cancel glare
Scanning Bed	Matte Dark / Non-Reflective Pad	Provides high optical contrast against card borders for OpenCV edge detection
Input Controller	Mechanical USB Numpad	Operator control interface for fast quantity entry and payload triggers
---
3. Repository File Structure & Module Map
```text
benihana77/mtg_scanner/
├── .gitignore               # Git ignore configuration (Python, Docker, DB files)
├── docker-compose.yml       # PostgreSQL container deployment
├── schema.sql               # Canonical DDL for scryfall_cards / inventory / price_history (see Section 6)
├── db.py                    # PostgreSQL connection management & parameterized queries (active)
├── hashing_engine.py        # [STUB] pHash helper only; camera/OpenCV capture not yet implemented
├── csv_pipeline.py          # ManaPool CSV intake, box/section allocation, relocation
├── manapool_client.py       # ManaPool API v1 client (orders, batch inventory push)
├── pull_list_optimizer.py   # Order ingestion, location optimization & printable pick sheet generator
├── tui.py                   # Curses terminal UI over the interim workflow
├── import_live_inventory.py # Fresh import of a ManaPool backup against scryfall_cards (preview, or --rebuild)
├── scryfall_mirror.py       # Local Scryfall mirror: bulk refresh, set/number + token-combo resolution
├── token_validation_report.py # Read-only report of tokens / non-tokens filed in the wrong place
├── special_cases_report.py # Printable check sheet of unusual cards at their planned locations
├── collection_pull.py       # Search / pull cards (or a decklist) for a personal collection, then sync ManaPool
├── repricer.py              # Repricer: ManaPool market-data pricing, risers, price history snapshots, pins
├── repricer.toml            # Repricer settings (adjustment, floor, review limits, riser thresholds)
├── audit.py                 # Physical audit: count sheets, count import, final ManaPool quantity push
├── audit_inventory.py       # One-time audit: unmapped set assignment + token extraction sheet (to be superseded by the token validation report)
├── test_interim_pipeline.py # unittest suite for the interim pipeline
└── Manapool Print Format - Set_Box Map.csv  # Set -> physical box (M1..M19) mapping
```
Module Responsibilities
`hashing_engine.py`: Intercepts camera frames, crops card boundaries using contour detection, generates perceptual hashes, and computes Hamming distance against Scryfall hash caches.
`db.py`: Executes parameterized SQL operations (`INSERT`/`UPDATE`) against the `scryfall_cards`, `inventory`, and `price_history` tables, and initializes the schema from `schema.sql`.
Scryfall mirror refresh (planned): downloads Scryfall's "Default Cards" bulk file and upserts it into `scryfall_cards`. Run manually from the TUI or CLI, e.g. before an intake that contains a new set.
`manapool_client.py`: Fetches unfulfilled orders and pushes absolute inventory quantities to the ManaPool API. `build_inventory_payload` builds the exact push payload (failing if database totals cannot be read, rather than falling back to per-item quantities); `preview-push` is a dry run that compares it with a baseline export and sends nothing.
`tui.py`: Keyboard-driven terminal front end for intake, fulfillment, browsing and relocation.
`csv_pipeline.py`: Manages temporary spreadsheet ingestion and ManaPool inventory sync prior to hardware CV integration.
`import_live_inventory.py`: Fresh import of a ManaPool inventory export against `scryfall_cards`, using the Set/Box map to assign boxes (preview by default, `--rebuild` to write).
`audit_inventory.py`: One-time audit that assigned boxes to unmapped sets and produced the token extraction walk sheet.
`collection_pull.py`: Pulls cards out of store inventory for a personal collection or deck (CLI and TUI). Picks the cheapest in-stock printing, oldest section first; holds back copies needed by unfulfilled ManaPool orders unless forced; deducts on confirmation and pushes new absolute totals for only the affected products (rows that reach 0 are kept at quantity 0 so the push sends 0). Without API credentials it writes a CSV of absolute quantities instead. Pulled cards are not tracked; returns go back through normal intake.
`pull_list_optimizer.py`: Ingests nightly order reports from ManaPool, joins with PostgreSQL inventory location metadata (`box_label`, `section_label`, `index_num`), calculates an optimal physical traversal path, and exports print-ready spreadsheets for fulfillment.
`docker-compose.yml`: Provisions the persistent PostgreSQL instance.
---
3a. Physical Storage Model
Boxes: Storage boxes are labelled `M1`–`M19`. Each set is assigned to one box via `Manapool Print Format - Set_Box Map.csv`; brand-new sets are assigned at intake by operator prompt.
Sections: Within a box, each set is split into append-only sections (`Sec-1`, `Sec-2`, …). Every intake batch for a set becomes the next section, alphabetized by card name and indexed `1..N`. Shelf order: names are compared case- and accent-insensitively (Éomer files under E; leading symbols like `+2 Mace` file first), and same-name printings go in numeric collector-number order, so the regular printing comes before showcase/other variants. Existing sections are never re-sorted. No divider slips are printed.
Special sections (box M19): `PROMOS` holds standalone promo sets (unchanged by the token design below).
Tokens (approved design):
- Detection: a card is a token if and only if its Scryfall `layout` (from `scryfall_cards`) is `token`, `double_faced_token` or `emblem`. No set-code or name heuristics. Art Series cards are not sold and get no special handling.
- Set code: tokens keep their real set code (`TMOM`, `DDR`, `TCMR`, …), so orders still resolve on set + collector number.
- `TOKENS` pseudo-set: all tokens behave as one set named `TOKENS`. Each intake batch's tokens, from any set, become one new append-only chunk with the next section label `TOKEN-1`, `TOKEN-2`, …, alphabetized by name → set code → collector number. A batch with no tokens creates no chunk.
- Token box: resolved like any set's box: from existing `TOKEN-n` rows in the database, else the `TOKENS` row of the Set/Box map (currently `M19`), else an operator prompt. Relocating `TOKENS` moves every `TOKEN-n` chunk together, keeping section labels and indexes.
- Double-sided token combos: ManaPool sells each double-sided token pairing as one product whose collector number joins the faces (`8-11` = front #8 / back #11 in the row's set; `17-TMOM-10` = front #17 in the row's set / back TMOM #10). Scryfall lists each face as a separate card, so a combo is stored as `inventory.scryfall_id` (front) + `inventory.back_scryfall_id` (back). Resolution order for a ManaPool set + number: (1) exact mirror match; (2) parse `front-back` / `front-SET-back` and resolve both faces; (3) if both faces are the same Scryfall card (e.g. `18-18`) and it is a `double_faced_token`, use that single card with no back; (4) otherwise refuse and list. A combo is a token if its front face is. Combos are sorted by ManaPool's product name, because ManaPool's name order does not always follow its number order (e.g. ZNR `5-10` is named "Construct // Drake" but #5 is the Drake).
- Singles only: import and intake only take `product_type = mtg_single` rows; other product types are skipped and counted in the summary.
- Unknown layout at intake: cannot occur. Every resolved card has a Scryfall layout; a row missing from the mirror stops the whole intake (nothing is written) and is listed, so the operator refreshes the mirror or fixes the CSV and re-runs.
Token clean-up (one-time):
1. Wipe the provisional Oct 7 MOM `Sec-2` test rows (box M18) from the database.
2. Validation report against the current database: a text walk sheet (box → set → section → index) plus CSV listing (a) tokens sitting in set boxes and (b) non-tokens sitting in `TOKENS`. Report only; it moves nothing.
3. Operator verifies and moves cards by hand. `special_cases_report.py` prints a check sheet of every unusual card (combos, emblems, helper cards, promos, meld names, symbol/accent names, same-name printings in text order, The List) at the box / section / index the fresh import will assign.
4. Fresh import (`python import_live_inventory.py --rebuild`; preview without the flag) from `backups/inventory-manapool-audited-20260927.csv` with real Scryfall IDs and the `layout` rule, using the shelf sort order above: one alphabetized `Sec-1` per set plus `TOKEN-1`, together with a list of sets that need physical re-sorting (sets that had several sections or gain/lose tokens).
---
4. Computer Vision & Perceptual Hashing Pipeline
```
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│ Camera Capture  │ ──> │ OpenCV Contour   │ ──> │ Perspective Warp    │
│ (Pi Camera 3)   │     │ Edge Detection   │     │ & Square Crop       │
└─────────────────┘     └──────────────────┘     └─────────────────────┘
                                                            │
                                                            ▼
┌─────────────────┐     ┌──────────────────┐     ┌─────────────────────┐
│ FastAPI WebSocket│ <── │ Hamming Distance │ <── │ Perceptual Hash     │
│ UI Verification │     │ Match (< Threshold)│     │ Generation          │
└─────────────────┘     └──────────────────┘     └─────────────────────┘
```
Hash Pre-Fetching: Prior to scanning a batch, the script reads card metadata for the 2–3 active set codes from the local `scryfall_cards` mirror and pre-generates their image hashes.
Frame Capture: Captures 1080p video feed from the Raspberry Pi camera.
Card Boundary Detection: OpenCV converts frame to grayscale, applies Gaussian blur, thresholding, and locates the primary 4-corner polygon contour on the dark background mat.
Perspective Transformation: Performs a 4-point perspective transform to rectify and crop the card into a normalized rectangle.
Image Hashing: Generates a perceptual hash (dHash/aHash) of the cropped image.
Lookup: Calculates Hamming distance against the pre-loaded Scryfall hash dictionary. Matches within the minimum distance threshold are pushed to the UI dashboard.
---
5. Human-in-the-Loop Control Scheme (USB Numpad)
To maximize throughput, card confirmation relies entirely on physical keypresses mapped via an input listener:
Key	Binding / Function	Payload / State Effect
`0` – `9`	Quantity Selection	Sets `quantity` field (default: `1`). Handles stacked duplicates easily.
`/`	Toggle Foil Status	Switches `is_foil` between `TRUE` and `FALSE`.
`*`	Cycle Printings	Cycles through candidate matches if algorithm guesses wrong set/printing.
`Enter`	Payload Trigger	Emits finalized JSON record, executes SQL `INSERT`/`UPDATE`, and resets buffer.
JSON Output Payload Format
Pressing `Enter` emits the following structured payload:
```json
{
  "card_name": "Fling",
  "set": "Dark Ascension",
  "condition": "NM",
  "foil": true,
  "quantity": 14,
  "timestamp": "2026-02-24T14:30:00"
}
```
---
6. Database Architecture & PostgreSQL Schema
The storage pipeline uses PostgreSQL deployed via Docker. The relational model decouples static Scryfall catalog data, physical inventory, and temporal pricing logs.
The canonical definition lives in `schema.sql`; this section mirrors it.
```sql
-- Local Scryfall Mirror (catalog source of truth)
-- Loaded from Scryfall's "Default Cards" bulk file and refreshed manually.
-- Intake and import resolve cards here; they never call the Scryfall API live.
CREATE TABLE IF NOT EXISTS scryfall_cards (
    scryfall_id VARCHAR(50) PRIMARY KEY,
    name TEXT NOT NULL,
    set_code VARCHAR(10) NOT NULL,          -- Upper-cased to match ManaPool exports
    set_type VARCHAR(30),
    collector_number VARCHAR(20) NOT NULL,
    rarity VARCHAR(20),
    layout VARCHAR(30) NOT NULL,            -- token / double_faced_token / emblem => token
    type_line TEXT,
    image_uri TEXT,
    raw JSONB NOT NULL,                     -- Full Scryfall card object
    refreshed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Physical Storefront Inventory Table
CREATE TABLE IF NOT EXISTS inventory (
    inventory_id SERIAL PRIMARY KEY,
    scryfall_id VARCHAR(50) NOT NULL REFERENCES scryfall_cards(scryfall_id),
    back_scryfall_id VARCHAR(50) REFERENCES scryfall_cards(scryfall_id),  -- Back face of a ManaPool double-sided token combo
    manapool_product_id VARCHAR(50),
    condition VARCHAR(10) DEFAULT 'NM',
    is_foil BOOLEAN DEFAULT FALSE,
    quantity INTEGER NOT NULL DEFAULT 1,
    manapool_listed_price DECIMAL(10, 2),
    box_label VARCHAR(50) NOT NULL,
    section_label VARCHAR(10) NOT NULL,     -- Sec-N for sets, TOKEN-N for token chunks
    index_num INTEGER NOT NULL,
    scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily Market Price Ledger Table
CREATE TABLE IF NOT EXISTS price_history (
    price_id SERIAL PRIMARY KEY,
    scryfall_id VARCHAR(50) REFERENCES scryfall_cards(scryfall_id),
    recorded_date DATE NOT NULL DEFAULT CURRENT_DATE,
    market_price_normal DECIMAL(10, 2),
    market_price_foil DECIMAL(10, 2)
);

-- Price history additions for the repricer (idempotent, so init_db migrates older databases).
-- One row per Scryfall card per snapshot date; prices are dollars, quantities are ManaPool-wide.
ALTER TABLE price_history ADD COLUMN IF NOT EXISTS low_price_normal DECIMAL(10, 2);   -- Lowest NM listing
ALTER TABLE price_history ADD COLUMN IF NOT EXISTS low_price_foil DECIMAL(10, 2);
ALTER TABLE price_history ADD COLUMN IF NOT EXISTS available_quantity INTEGER;         -- Copies listed on ManaPool
ALTER TABLE price_history ADD COLUMN IF NOT EXISTS listed_price_normal DECIMAL(10, 2); -- Our listing price at snapshot
ALTER TABLE price_history ADD COLUMN IF NOT EXISTS listed_price_foil DECIMAL(10, 2);
ALTER TABLE price_history ADD COLUMN IF NOT EXISTS feed_as_of TIMESTAMP;               -- ManaPool feed timestamp

-- Repricer pins: products the repricer must not change (or must hold at fixed_price)
CREATE TABLE IF NOT EXISTS price_pins (
    manapool_product_id VARCHAR(50) PRIMARY KEY,
    fixed_price DECIMAL(10, 2),              -- NULL = keep whatever price is listed
    note TEXT,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);
```
Catalog model: `scryfall_cards` replaces the former `cards` table and is the single source of card facts (name, set, number, rarity, layout, image). `inventory` and `price_history` reference it directly; card facts are obtained by joining, never copied.
Double-sided token combos: `back_scryfall_id` holds the back face of a ManaPool combo product and is NULL for every other card (see Section 3a).
No synthetic IDs: a card that cannot be resolved against `scryfall_cards` (after a refresh) is refused at intake/import and listed in the run summary. A `custom_cards` table will only be added if such rows turn out to be a real need.
`image_hash`: the only non-Scryfall catalog field. It is unused until Phase 3 and will get its own table (`scryfall_id` → hash) at that point.
Migration: existing databases still have the old `cards` table and the synthetic `mp-<set>-<num>` IDs. They are replaced during the token clean-up fresh import (Section 3a), which wipes `inventory`/`cards` and reloads against `scryfall_cards`.
Repricer (`repricer.py`, settings in `repricer.toml`) — replaces ManaPool's bulk pricer
Market data: ManaPool's public `GET /prices/singles` feed (one unauthenticated request covering every in-stock single, refreshed about hourly, keyed by Scryfall ID, prices in cents). Feed data older than `[feed] max_age_hours` is refused.
Base price per listing: max(lowest NM listing, market price) × (1 + adjustment_pct/100) — default −1%, matching ManaPool's "High of Low Listed & Market Price −1%" — using foil fields for foils, rounded half up to the cent, never below the $0.15 floor (ManaPool's minimum). Cards with neither anchor (including double-sided combo tokens) are skipped and keep their price.
Review: a change is applied automatically unless it exceeds BOTH `max_auto_change_pct` and `max_auto_change_dollars` (default 30% and $0.50); those go to a review list (accept all / reject all / one by one with optional custom price).
Risers: flagged if any signal fires — lowest listing ≥ 1.5× market; ManaPool-wide available quantity down ≥ 50% within 7 days; market up ≥ 50% and ≥ $0.25 within 7 days. Risers use the normal formula and only need review when over the limits; they are highlighted in the report.
Pins: `price_pins` (by ManaPool product ID) hold the listed price or a fixed price; managed via CLI (`pin`/`unpin`/`pins`) and the TUI.
Run flow (manual, CLI `run` or TUI [R]): drift check against live prices (`GET /seller/inventory`, cursor-paged) — refuses to push if any live price differs from the database (`sync --apply` copies live prices in) → plan → review → confirm → price-only push (`quantity: null`, batches of 2,000, 429/Retry-After honoured) → read back and update `manapool_listed_price` for verified products → report (`exports/reprice_*.txt/.csv`). `--preview` plans and reports without pushing.
Nightly snapshot (cron, 3:00 AM): `repricer.py snapshot` upserts one `price_history` row per inventory card per day (combos excluded) and never changes prices. Automatic nightly repricing is deferred until manual runs are validated.
---
7. Nightly Order Pull List Optimizer (`pull_list_optimizer.py`)
To optimize physical order fulfillment, a nightly batch job processes incoming ManaPool orders and generates an optimized physical pick sheet.
```
┌─────────────────────────┐
│ Incoming ManaPool Orders│
│ (Nightly Batch / API)   │
└────────────┬────────────┘
             │
             ▼
┌─────────────────────────┐     ┌───────────────────────────────────┐
│ Query PostgreSQL DB     │ ──> │ Location-Aware Traversal Optimizer│
│ (inventory ⋈ scryfall)  │     │ Sort: Box -> Section -> Index     │
└─────────────────────────┘     └─────────────────┬─────────────────┘
                                                  │
                                                  ▼
┌─────────────────────────┐     ┌───────────────────────────────────┐
│ Decrement / Allocate    │ <── │ Export Printable Pick Sheet       │
│ Inventory Quantities    │     │ (.xlsx / PDF with Checkboxes)     │
└─────────────────────────┘     └───────────────────────────────────┘
```
Workflow Mechanics
Order Ingestion: Triggers nightly to fetch sold order line items from ManaPool (via API or CSV order export).
Database Resolution: Performs SQL `JOIN` between order items, `inventory`, and `scryfall_cards` to resolve physical storage coordinates (`box_label`, `section_label`, `index_num`).
Location Sorting & Traversal Optimization:
Regular sections: `box_label` (natural numeric order, M1 → M19) → `set_code` (alphabetical within box) → `section_label` (natural order, Sec-1 → Sec-2) → `index_num`.
Token chunks (`TOKEN-n`): `box_label` → `section_label` (TOKEN-1 → TOKEN-2) → `index_num`, ignoring set code, since each chunk mixes many sets.
Allocation is FIFO: when a card exists in several sections or token chunks, the oldest is drawn from first.
Minimizes human physical movement across storage racks by sorting items sequentially along the physical layout path.
Stock Reservation & Deduction: Updates or decrements `inventory.quantity` to reflect pulled cards, flagging out-of-stock items if discrepancies occur.
Printable Output Generation: Exports a print-ready pick sheet for rapid manual picking. (Current implementation: plain-text table at `exports/pick_sheet_<date>.txt` with an out-of-stock section; `.xlsx`/PDF export is not built.)
Pick Seq	Box	Section	Index	Card Name	Set	Collector #	Foil	Qty to Pull	Order ID	Collected [ ]
1	Box-01	Sec-A	004	Fling	DKA	087	Yes	2	#MP-10492	[  ]
2	Box-01	Sec-A	012	Lightning Bolt	CLB	187	No	4	#MP-10492	[  ]
3	Box-02	Sec-C	001	Sol Ring	C21	263	No	1	#MP-10495	[  ]
---
8. Implementation Roadmap
[ ] Phase 1: Physical Jig Assembly — not started  
Mount copy stand, Pi Camera Module 3, microscope ring light, Selens cross-polarizing film, and USB numpad.
[ ] Phase 2: OpenCV & Frame Grabber Foundation — not started  
Initialize Raspberry Pi OS 64-bit environment. Write frame capture script in OpenCV.
[ ] Phase 3: Image Hashing Engine (`hashing_engine.py`) — stub only  
Build Scryfall API hash pre-fetcher, card contour detector, perspective transform, and dHash matcher.
[ ] Phase 4: Web Dashboard & Numpad Controller — not started (interim curses TUI exists)  
Implement FastAPI WebSockets server to stream real-time camera frames and candidate cards to companion UI. Map mechanical numpad keybindings.
[~] Phase 5: Database Pipeline & Interim CSV Bridge (`db.py`, `csv_pipeline.py`, `import_live_inventory.py`) — built; repricer built (manual runs + nightly history snapshot); automatic nightly repricing not yet enabled  
Deploy Dockerized PostgreSQL server, connect FastAPI pipeline via `psycopg2`/`SQLAlchemy`, build interim two-way ManaPool CSV sync engine, and implement daily 3:00 AM auto-pricer job.
Token & catalog rework — done (2026-10-19): `scryfall_cards` mirror + refresh, combo-token resolution, validation and special-case reports, fresh import (`--rebuild`) run against the audited backup, token-aware intake (`TOKEN-n` chunks), pick-sheet token walk order and combo matching, `TOKENS` relocation, divider slips removed, integration tests isolated in `mtg_inventory_test`.
[~] Phase 6: Nightly Pull List Optimizer & Fulfillment Engine (`pull_list_optimizer.py`) — built, not yet verified (text pick sheet; not scheduled nightly)  
Build ManaPool order ingestion module, write location-sorting query engine (`box_label` $\rightarrow$ `section_label` $\rightarrow$ `index_num`), automate inventory quantity deductions, and implement print-ready spreadsheet exporter.
