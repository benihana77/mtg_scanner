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
├── hashing_engine.py        # Camera capture, OpenCV edge detection & perceptual hashing
├── db_pipeline.py           # PostgreSQL connection management & database operations
├── csv_pipeline.py          # Two-way ManaPool CSV import/export pipeline (interim scanning replacement)
├── migrate_inventory.py     # Legacy CSV data importer for PostgreSQL schema
└── pull_list_optimizer.py   # Nightly order ingestion, location optimization & printable pull list generator
```
Module Responsibilities
`hashing_engine.py`: Intercepts camera frames, crops card boundaries using contour detection, generates perceptual hashes, and computes Hamming distance against Scryfall hash caches.
`db_pipeline.py`: Executes parameterized SQL operations (`INSERT`/`UPDATE`) against the `cards`, `inventory`, and `price_history` tables.
`csv_pipeline.py`: Manages temporary spreadsheet ingestion and ManaPool inventory sync prior to hardware CV integration.
`migrate_inventory.py`: Reads offline JSON/CSV scanning payloads and migrates them into the relational database.
`pull_list_optimizer.py`: Ingests nightly order reports from ManaPool, joins with PostgreSQL inventory location metadata (`box_label`, `section_label`, `index_num`), calculates an optimal physical traversal path, and exports print-ready spreadsheets for fulfillment.
`docker-compose.yml`: Provisions the persistent PostgreSQL instance.
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
Hash Pre-Fetching: Prior to scanning a batch, the script calls the Scryfall API to fetch card metadata and pre-generate image hashes only for the 2–3 active set codes.
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
```sql
-- Static Scryfall Card Catalog Table
CREATE TABLE cards (
    scryfall_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    set_code VARCHAR(10) NOT NULL,
    collector_number VARCHAR(10),
    rarity VARCHAR(20)
);

-- Physical Storefront Inventory Table
CREATE TABLE inventory (
    inventory_id SERIAL PRIMARY KEY,
    scryfall_id VARCHAR(50) REFERENCES cards(scryfall_id),
    condition VARCHAR(10) DEFAULT 'NM',
    is_foil BOOLEAN DEFAULT FALSE,
    quantity INTEGER NOT NULL DEFAULT 1,
    manapool_listed_price DECIMAL(10, 2),
    box_label VARCHAR(50),
    section_label VARCHAR(10),
    index_num INTEGER,
    scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily Market Price Ledger Table
CREATE TABLE price_history (
    price_id SERIAL PRIMARY KEY,
    scryfall_id VARCHAR(50) REFERENCES cards(scryfall_id),
    recorded_date DATE NOT NULL DEFAULT CURRENT_DATE,
    market_price_normal DECIMAL(10, 2),
    market_price_foil DECIMAL(10, 2)
);
```
Automated Price Sync Workflow (3:00 AM Cron Job)
Fetch Active Stock: Secondary Python cron script queries `inventory` for all distinct `scryfall_id` values currently in stock.
Retrieve API Rates: Queries Scryfall/TCGPlayer APIs for latest normal and foil market prices.
Log History: Inserts new price rows into `price_history`.
Sync ManaPool: Compares current market rates against `manapool_listed_price` in `inventory` and posts price updates to the ManaPool API if threshold deltas are met.
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
│ (inventory & cards JOIN)│     │ Sort: Box -> Section -> Index     │
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
Database Resolution: Performs SQL `JOIN` between order items, `inventory`, and `cards` to resolve physical storage coordinates (`box_label`, `section_label`, `index_num`).
Location Sorting & Traversal Optimization:
Primary Sort: `box_label` (alphabetical / numerical box sequence).
Secondary Sort: `section_label` (alphabetical section within box).
Tertiary Sort: `index_num` (positional card sequence within section).
Minimizes human physical movement across storage racks by sorting items sequentially along the physical layout path.
Stock Reservation & Deduction: Updates or decrements `inventory.quantity` to reflect pulled cards, flagging out-of-stock items if discrepancies occur.
Printable Output Spreadsheet Generation: Formats and exports a clean, print-ready spreadsheet (`.xlsx`) structured for rapid manual physical picking:
Pick Seq	Box	Section	Index	Card Name	Set	Collector #	Foil	Qty to Pull	Order ID	Collected [ ]
1	Box-01	Sec-A	004	Fling	DKA	087	Yes	2	#MP-10492	[  ]
2	Box-01	Sec-A	012	Lightning Bolt	CLB	187	No	4	#MP-10492	[  ]
3	Box-02	Sec-C	001	Sol Ring	C21	263	No	1	#MP-10495	[  ]
---
8. Implementation Roadmap
[x] Phase 1: Physical Jig Assembly  
Mount copy stand, Pi Camera Module 3, microscope ring light, Selens cross-polarizing film, and USB numpad.
[ ] Phase 2: OpenCV & Frame Grabber Foundation  
Initialize Raspberry Pi OS 64-bit environment. Write frame capture script in OpenCV.
[ ] Phase 3: Image Hashing Engine (`hashing_engine.py`)  
Build Scryfall API hash pre-fetcher, card contour detector, perspective transform, and dHash matcher.
[ ] Phase 4: Web Dashboard & Numpad Controller  
Implement FastAPI WebSockets server to stream real-time camera frames and candidate cards to companion UI. Map mechanical numpad keybindings.
[ ] Phase 5: Database Pipeline & Interim CSV Bridge (`db_pipeline.py`, `csv_pipeline.py`, `migrate_inventory.py`)  
Deploy Dockerized PostgreSQL server, connect FastAPI pipeline via `psycopg2`/`SQLAlchemy`, build interim two-way ManaPool CSV sync engine, and implement daily 3:00 AM auto-pricer job.
[ ] Phase 6: Nightly Pull List Optimizer & Fulfillment Engine (`pull_list_optimizer.py`)  
Build ManaPool order ingestion module, write location-sorting query engine (`box_label` $\rightarrow$ `section_label` $\rightarrow$ `index_num`), automate inventory quantity deductions, and implement print-ready spreadsheet exporter.
