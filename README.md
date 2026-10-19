# MTG Inventory Management & Fulfillment Station

A high-velocity inventory management station and fulfillment optimizer for Magic: The Gathering card singles, designed for seamless integration with **ManaPool** and backed by **PostgreSQL**.

> **Status:** In design and development. The inventory and fulfillment software is built but not yet verified end to end. The camera/scanner hardware pipeline is not built yet; card intake currently uses ManaPool CSV exports.

---

## Physical Storage Model

- Storage boxes are labelled **M1–M19**. `Manapool Print Format - Set_Box Map.csv` maps each set to its box.
- Each set lives in one box, split into append-only **sections** (`Sec-1`, `Sec-2`, …). Each intake batch becomes the next section, sorted in shelf order and indexed `1..N`. Shelf order ignores case and accents (Éomer files under E), puts leading symbols first, and lists same-name printings by collector number (regular printing before showcase variants).
- **Tokens** (Scryfall layout `token`, `double_faced_token` or `emblem`) form a shelf set called `TOKENS`, currently in box **M19**. Each intake batch's tokens, from any set, become the next append-only chunk (`TOKEN-1`, `TOKEN-2`, …). Tokens keep their real set code.
- Box **M19** also holds `PROMOS` (standalone promo sets).

---

## Key Features

1. **Interactive Curses TUI (`tui.py`)**:
   - Keyboard-driven terminal console with real-time database and API indicators.
   - Menu: batch intake, push inventory to ManaPool, pick sheet from live API orders, pick sheet from a picklist CSV, browse inventory, box shuffle, diagnostics, and Scryfall mirror refresh.

2. **ManaPool Batch Ingestion (`csv_pipeline.py`)**:
   - Ingests ManaPool set spreadsheets (`multiple_sets_all_products_*.csv`).
   - Filters in-stock singles (`quantity > 0`, `product_type = mtg_single`).
   - Resolves every card against the local Scryfall mirror (`scryfall_cards`), including ManaPool double-sided token combos (`8-11`, `17-TMOM-10`). If any row is missing from the mirror, nothing is ingested and the rows are listed.
   - Routes tokens from every set into one new `TOKEN-n` chunk in the token box.
   - Auto-resolves each set's box from existing inventory (prompts for brand-new sets).
   - Auto-allocates next physical section (`Sec-1`, `Sec-2`, ...) for each (Box, Set) pair.
   - Sorts each section in shelf order and assigns sequential index numbers (`1, 2, 3... N`).
   - Exports a **ManaPool Upload CSV** (`exports/manapool_upload_*.csv`), ready for web dashboard upload ("Add to existing").

3. **Box Capacity Management & "Box Shuffle" (`relocate`)**:
   - Easily move an entire set or a specific section from one storage box to another when a box runs out of capacity.
   - `--set TOKENS` moves every token chunk together, keeping section labels and index numbers.

4. **Order Pull List Optimizer (`pull_list_optimizer.py`)**:
   - Ingests ManaPool picklists either from **live ManaPool API** (`GET /seller/orders?is_unfulfilled=true`) or local CSV exports (`picklist-*.csv`).
   - Resolves order lines through the Scryfall mirror (same rules as intake) and physical coordinates: `Box -> Set -> Section -> Index` (token chunks: `Box -> Section -> Index`).
   - **Zero-Backtracking Path Optimization**: Sorts the picking sequence sequentially along the warehouse walking route.
   - Generates printable pick sheets (`exports/pick_sheet_*.txt`) with `[ ]` checkboxes for clipboard picking.
   - Supports stock deduction (`--commit`) directly updating PostgreSQL.

5. **ManaPool API v1 Integration (`manapool_client.py`)**:
   - Direct batch inventory updates via `POST /seller/inventory/product` (up to 2,000 items per request).
   - Automated unfulfilled order retrieval and item parsing.
   - Graceful fallback to offline CSV mode when API credentials are not set.

6. **Scryfall Mirror (`scryfall_mirror.py`)**:
   - `python scryfall_mirror.py refresh` loads Scryfall's Default Cards bulk file (about a minute); `status` shows its size and age. Refresh before intaking a newly released set.

7. **Setup & Audit Tools**:
   - `import_live_inventory.py`: fresh import of a ManaPool inventory export against the Scryfall mirror and Set/Box map. Previews by default; `--rebuild` drops and recreates the inventory tables atomically.
   - `token_validation_report.py`: read-only report of tokens filed in set sections, non-tokens in token sections, and rows missing from the mirror.
   - `special_cases_report.py`: printable check sheet of unusual cards (combos, emblems, helper cards, promos, accent/symbol names, same-name printings, The List) at their planned locations.
   - `audit_inventory.py`: one-time audit that assigned boxes to unmapped sets and produced the original token extraction walk sheet.
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
python scryfall_mirror.py refresh
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

# Move every token chunk to a new box
python csv_pipeline.py relocate --source M19 --target M20 --set TOKENS
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
python -m unittest test_interim_pipeline test_token_validation -v
```
The integration tests run against a separate `mtg_inventory_test` database (override with `TEST_DB_NAME`), which they create and reset themselves; the live database is never touched.

---

## Known Limitations
- **Legacy scripts:** `migrate_inventory.py` and `db_pipeline.py` still target the removed `cards` table and no longer run.
- **ManaPool combo names:** a combo token's displayed name comes from Scryfall's face order, which can differ from ManaPool's product name (e.g. ZNR `5-10`).
