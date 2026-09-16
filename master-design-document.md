# Master Design Document & Technical Specification

> **Target Audience:** LLM Coding Agents (Cursor, Claude Code, GitHub Copilot) & Systems Engineers  
> **Project:** Custom MTG Card Scanning & Inventory Management Station  
> **Repository:** `benihana77/mtg_scanner`

---

## 🤖 Prompt Engineering Directives for LLM Coding Agents

When executing tasks within this repository, all LLM coding agents **MUST** adhere to the following operational directives:

1. **Role & Mindset**: Act as a Senior Systems and Computer Vision Engineer specializing in embedded Linux (Raspberry Pi), real-time OpenCV processing, FastAPI, and PostgreSQL database pipelines.
2. **Hardware-First Constraint Rule**: **DO NOT** attempt to replace physical hardware constraints (e.g., cross-polarization, dark scanning mats, pre-sorted batches) with complex machine learning or auto-grading algorithms. Software design MUST assume fixed lighting and NM (Near Mint) condition hardcoding.
3. **Incremental Execution**: Implement features step-by-step according to Section 8 (Implementation Roadmap). Do not refactor unrelated files or modules unless explicitly instructed.
4. **Code Quality Standards**:
   - Write clean, production-grade Python 3.12+ code.
   - Enforce strict type hints (`typing` module) across all function signatures.
   - Include clear docstrings (Google style) for all classes, methods, and API routes.
   - Maintain defensive error handling, specifically for camera feed drops, Scryfall API rate limits, and database connection retries.
5. **Database Integrity**: Do not modify table schema definitions or column names without explicit instructions. Strictly preserve the schema specified in Section 6.

---

## 1. Project Overview & System Philosophy

This project defines a custom, hardware-anchored Magic: The Gathering (MTG) card scanning and inventory listing system designed to streamline bulk card cataloging for a ManaPool storefront.

### Core Philosophy
Commercial scanning apps often bottleneck high-volume bulk processing due to foil glare, set misidentification, and reliance on touchscreen interactions. This project replaces software complexity with physical environment controls and targeted scope bounds:
- ** glare elimination**: Cross-polarized lighting cancels reflections physically rather than in software.
- **Search Space Reduction**: Matching is scoped exclusively to pre-fetched image hashes of the 2–3 active sets being scanned.
- **Human-in-the-Loop Velocity**: A physical USB numpad allows instant quantity entry and visual confirmation without taking hands off the card stack.

### Operational Constraints & Assumptions
- **Condition**: All scanned cards are assumed to be **Near Mint (NM)**. Automated condition grading is explicitly out of scope.
- **Batching**: Cards are pre-sorted into alphabetized stacks of ~1,000 cards across 2 to 3 target sets.
- **Identical Stacks**: The workflow handles consecutive identical cards (e.g., 10+ copies of the same card) via manual quantity selection on a numpad, avoiding frame-differencing failure modes.

---

## 2. Hardware Architecture & Rig Specifications

| Component | Specification | Technical Function |
| :--- | :--- | :--- |
| **Compute Node** | Raspberry Pi 4 (or Pi 3), 64-bit OS | Hosts OpenCV engine, FastAPI server, and numpad listener |
| **Camera Module** | Raspberry Pi Camera Module 3 Standard (12MP Sony IMX708, I2C autofocus actuator, 75° FoV) or 1080p USB Webcam | Mounted downward on a copy stand 8–12 inches overhead |
| **Illumination** | Microscope LED Ring Light | Surrounds the camera lens to provide direct, uniform lighting |
| **Optics / Polarization** | Selens Linear Polarizing Sheet | Two polarizing sheets placed 90° cross-rotated (one over lens, one over ring light) to cancel glare |
| **Scanning Bed** | Matte Dark / Non-Reflective Pad | Provides high optical contrast against card borders for OpenCV edge detection |
| **Input Controller** | Mechanical USB Numpad | Operator control interface for fast quantity entry and payload triggers |

---

## 3. Repository File Structure & Module Map

```text
benihana77/mtg_scanner/
├── .gitignore             # Git ignore configuration (Python, Docker, DB files)
├── docker-compose.yml     # PostgreSQL container deployment
├── hashing_engine.py      # Camera capture, OpenCV edge detection & perceptual hashing
├── db_pipeline.py         # PostgreSQL connection management & database operations
└── migrate_inventory.py   # Legacy CSV data importer for PostgreSQL schema
```

### Module Responsibilities
- **`hashing_engine.py`**: Intercepts camera frames, crops card boundaries using contour detection, generates perceptual hashes, and computes Hamming distance against Scryfall hash caches.
- **`db_pipeline.py`**: Executes parameterized SQL operations (`INSERT`/`UPDATE`) against the `cards`, `inventory`, and `price_history` tables.
- **`migrate_inventory.py`**: Reads offline JSON/CSV scanning payloads and migrates them into the relational database.
- **`docker-compose.yml`**: Provisions the persistent PostgreSQL instance.

---

## 4. Computer Vision & Perceptual Hashing Pipeline

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

1. **Hash Pre-Fetching**: Prior to scanning a batch, the script calls the Scryfall API to fetch card metadata and pre-generate image hashes *only* for the 2–3 active set codes.
2. **Frame Capture**: Captures 1080p video feed from the Raspberry Pi camera.
3. **Card Boundary Detection**: OpenCV converts frame to grayscale, applies Gaussian blur, thresholding, and locates the primary 4-corner polygon contour on the dark background mat.
4. **Perspective Transformation**: Performs a 4-point perspective transform to rectify and crop the card into a normalized rectangle.
5. **Image Hashing**: Generates a perceptual hash (dHash/aHash) of the cropped image.
6. **Lookup**: Calculates Hamming distance against the pre-loaded Scryfall hash dictionary. Matches within the minimum distance threshold are pushed to the UI dashboard.

---

## 5. Human-in-the-Loop Control Scheme (USB Numpad)

To maximize throughput, card confirmation relies entirely on physical keypresses mapped via an input listener:

| Key | Binding / Function | Payload / State Effect |
| :---: | :--- | :--- |
| **`0` – `9`** | Quantity Selection | Sets `quantity` field (default: `1`). Handles stacked duplicates easily. |
| **`/`** | Toggle Foil Status | Switches `is_foil` between `TRUE` and `FALSE`. |
| **`*`** | Cycle Printings | Cycles through candidate matches if algorithm guesses wrong set/printing. |
| **`Enter`** | Payload Trigger | Emits finalized JSON record, executes SQL `INSERT`/`UPDATE`, and resets buffer. |

### JSON Output Payload Format
Pressing **`Enter`** emits the following structured payload:
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

## 6. Database Architecture & PostgreSQL Schema

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

### Automated Price Sync Workflow (3:00 AM Cron Job)
1. **Fetch Active Stock**: Secondary Python cron script queries `inventory` for all distinct `scryfall_id` values currently in stock.
2. **Retrieve API Rates**: Queries Scryfall/TCGPlayer APIs for latest normal and foil market prices.
3. **Log History**: Inserts new price rows into `price_history`.
4. **Sync ManaPool**: Compares current market rates against `manapool_listed_price` in `inventory` and posts price updates to the ManaPool API if threshold deltas are met.

---

## 7. Implementation Roadmap

- [x] **Phase 1: Physical Jig Assembly**  
  Mount copy stand, Pi Camera Module 3, microscope ring light, Selens cross-polarizing film, and USB numpad.
- [ ] **Phase 2: OpenCV & Frame Grabber Foundation**  
  Initialize Raspberry Pi OS 64-bit environment. Write frame capture script in OpenCV.
- [ ] **Phase 3: Image Hashing Engine (`hashing_engine.py`)**  
  Build Scryfall API hash pre-fetcher, card contour detector, perspective transform, and dHash matcher.
- [ ] **Phase 4: Web Dashboard & Numpad Controller**  
  Implement FastAPI WebSockets server to stream real-time camera frames and candidate cards to companion UI. Map mechanical numpad keybindings.
- [ ] **Phase 5: Database Pipeline & Integration (`db_pipeline.py`, `migrate_inventory.py`)**  
  Deploy Dockerized PostgreSQL server, connect FastAPI pipeline via `psycopg2`/`SQLAlchemy`, write JSON migration script, and implement daily 3:00 AM auto-pricer job.
