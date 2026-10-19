# MTG Inventory Management & Fulfillment Station

A high-velocity inventory management station and fulfillment optimizer for Magic: The Gathering card singles, designed for seamless integration with **ManaPool** and backed by **PostgreSQL**.

> **Status:** In design and development. The inventory and fulfillment software is built but not yet verified end to end. The camera/scanner hardware pipeline is not built yet; card intake currently uses ManaPool CSV exports.

---

## Physical Storage Model

- Storage boxes are labelled **M1–M19**. `Manapool Print Format - Set_Box Map.csv` maps each set to its box.
- Each set lives in one box, split into append-only **sections** (`Sec-1`, `Sec-2`, …). Each intake batch becomes the next section, alphabetized and indexed `1..N`, with a printed divider slip.
- Box **M19** also holds two special sections: `TOKENS` (all tokens/emblems, alphabetized across token sets) and `PROMOS` (standalone promo sets).

---

## Key Features

1. **Interactive Curses TUI (`tui.py`)**:
   - Keyboard-driven terminal console with real-time database and API indicators.
   - Menu: batch intake, push inventory to ManaPool, pick sheet from live API orders, pick sheet from a picklist CSV, browse inventory, box shuffle, and diagnostics.

2. **ManaPool Batch Ingestion (`csv_pipeline.py`)**:
   - Ingests ManaPool set spreadsheets (`multiple_sets_all_products_*.csv`).
   - Filters rows with `quantity > 0`.
   - Resolves card metadata with local PostgreSQL caching and Scryfall API fallback.
   - Auto-resolves each set's box from existing inventory (prompts for brand-new sets).
   - Auto-allocates next physical section (`Sec-1`, `Sec-2`, ...) for each (Box, Set) pair.
   - **Strict Alphabetical Sorting**: Alphabetizes cards by name within each section and assigns sequential index numbers (`1, 2, 3... N`).
   - Exports:
     - **ManaPool Upload CSV** (`exports/manapool_upload_*.csv`): Ready for immediate web dashboard upload ("Add to existing").
     - **Physical Divider Slip** (`exports/divider_*.txt`): Print-ready table to place behind physical box dividers.

3. **Box Capacity Management & "Box Shuffle" (`relocate`)**:
   - Easily move an entire set or a specific section from one storage box to another when a box runs out of capacity.
   - Automatically updates database records and regenerates physical divider slips for the destination box.

4. **Order Pull List Optimizer (`pull_list_optimizer.py`)**:
   - Ingests ManaPool picklists either from **live ManaPool API** (`GET /seller/orders?is_unfulfilled=true`) or local CSV exports (`picklist-*.csv`).
   - Resolves physical coordinates: `Box -> Set -> Section -> Index`.
   - **Zero-Backtracking Path Optimization**: Sorts the picking sequence sequentially along the warehouse walking route.
   - Generates printable pick sheets (`exports/pick_sheet_*.txt`) with `[ ]` checkboxes for clipboard picking.
   - Supports stock deduction (`--commit`) directly updating PostgreSQL.

5. **ManaPool API v1 Integration (`manapool_client.py`)**:
   - Direct batch inventory updates via `POST /seller/inventory/product` (up to 2,000 items per request).
   - Automated unfulfilled order retrieval and item parsing.
   - Graceful fallback to offline CSV mode when API credentials are not set.

6. **One-Time Setup Tools**:
   - `import_live_inventory.py`: seeds PostgreSQL from a ManaPool inventory export plus the Set/Box map (supports `--dry-run`, `--clear-existing`).
   - `audit_inventory.py`: assigns boxes to unmapped sets and generates the token extraction walk sheet and token-box divider slip.
   - `migrate_inventory.py` (superseded by `import_live_inventory.py`), `db_pipeline.py` and `hashing_engine.py` (early scanner work; not part of the current workflow).

---

## Quick Start

### 1. Environment Setup
```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Start PostgreSQL and Initialize the Schema
```bash
docker compose up -d
python db.py   # runs schema.sql
```

### 3. Configure Environment (`.env`)
```ini
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mtg_inventory
DB_USER=mtg_admin
DB_PASS=password

# Optional: For live ManaPool API integration
MANAPOOL_EMAIL=your_email@example.com
MANAPOOL_API_TOKEN=mpat_your_token_here
```

### 4. Launch the Curses TUI
```bash
python tui.py
```

---

## Command-Line Usage (CLI)

### Ingest ManaPool Set Batch
```bash
# Ingest batch (box auto-resolved per set, next section auto-allocated)
python csv_pipeline.py ingest multiple_sets_singles_2026-10-08T00-51-00.csv

# Force a specific box
python csv_pipeline.py ingest batch.csv --box M18

# Ingest and push directly to ManaPool API
python csv_pipeline.py ingest batch.csv --push-api
```

### Relocate Set Between Boxes (Box Shuffle)
```bash
# Move set ZNR from M3 to M4 when M3 is full
python csv_pipeline.py relocate --source M3 --target M4 --set ZNR

# Move a single section
python csv_pipeline.py relocate --source M3 --target M4 --set ZNR --section Sec-1
```

### Browse Physical Inventory
```bash
python csv_pipeline.py list
python csv_pipeline.py list --box M1
```

### Run Order Fulfillment Picklist
```bash
# Preview optimized pick sheet from CSV (no stock deduction)
python pull_list_optimizer.py picklist-2026-09-20.csv

# Process CSV and deduct pulled cards from PostgreSQL inventory
python pull_list_optimizer.py picklist-2026-09-20.csv --commit

# Pull unfulfilled orders directly from ManaPool API and deduct
python pull_list_optimizer.py --api --commit
```

---

## Running Automated Tests
```bash
python -m unittest test_interim_pipeline.py -v
```

---

## Known Limitations
- **Tokens:** batch intake has no token handling yet, so new tokens get a `Sec-N` under their own set code instead of joining the M19 token section. The token handling design is still open (see the project status doc).
- **Pick order in `TOKENS`:** the pick sheet sorts by set code before index, so it walks the token section set by set rather than top to bottom.
- **Catalog metadata:** cards seeded by the live import use synthetic IDs (`mp-<set>-<num>`) with no Scryfall layout or image data.
