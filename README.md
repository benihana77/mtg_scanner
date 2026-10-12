# MTG Inventory Management & Fulfillment Station

A high-velocity inventory management station and fulfillment optimizer for Magic: The Gathering card singles, designed for seamless integration with **ManaPool** and backed by **PostgreSQL**.

---

## Key Features

1. **Interactive Curses TUI (`tui.py`)**:
   - Keyboard-driven terminal console with real-time database and API indicators.
   - Jump directly to batch intake, live order fulfillment, physical box browsing, or box shuffling.

2. **ManaPool Batch Ingestion (`csv_pipeline.py`)**:
   - Ingests ManaPool set spreadsheets (`multiple_sets_all_products_*.csv`).
   - Filters rows with `quantity > 0`.
   - Resolves card metadata with local PostgreSQL caching and Scryfall API fallback.
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

---

## Quick Start

### 1. Environment Setup
```bash
# Activate virtual environment
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 2. Configure Environment (`.env`)
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

### 3. Launch the Curses TUI
```bash
python tui.py
```

---

## Command-Line Usage (CLI)

### Ingest ManaPool Set Batch
```bash
# Ingest batch into Box-1 (auto-allocates next section, e.g. Sec-1)
python csv_pipeline.py ingest multiple_sets_all_products_2026-09-20T00-27-05.csv --box Box-1

# Ingest and push directly to ManaPool API
python csv_pipeline.py ingest batch.csv --box Box-1 --push-api
```

### Relocate Set Between Boxes (Box Shuffle)
```bash
# Move set ZNR from Box-1 to Box-2 when Box-1 is full
python csv_pipeline.py relocate --source Box-1 --target Box-2 --set ZNR

# Move a single section
python csv_pipeline.py relocate --source Box-1 --target Box-2 --set ZNR --section Sec-1
```

### Browse Physical Inventory
```bash
python csv_pipeline.py list
python csv_pipeline.py list --box Box-1
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
