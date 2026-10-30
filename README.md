# MTG Inventory Management & Fulfillment Station

A high-velocity inventory management station and fulfillment optimizer for Magic: The Gathering card singles, designed for seamless integration with **ManaPool** and backed by **PostgreSQL**.

> **Status:** In use. As of Oct 2026 the database and ManaPool match after a physical audit, and the repricer and quantity pushes have run against the live store with read-back verification. The camera/scanner hardware pipeline is not built yet; card intake currently uses ManaPool CSV exports.

---

## Repository Layout

```text
mtg_scanner/
├── tui.py                 # Curses TUI: python tui.py
├── station/               # Daily tools and the shared library: python -m station.<module>
│   ├── db.py, schema.sql  #   PostgreSQL connection and schema
│   ├── scryfall_mirror.py #   Local Scryfall mirror
│   ├── manapool_client.py #   ManaPool API client and push tools
│   ├── csv_pipeline.py    #   Intake, relocation, inventory listing
│   ├── pull_list_optimizer.py, collection_pull.py  # Order picking, personal pulls
│   ├── repricer.py, audit.py                       # Pricing, physical audits
│   └── import_live_inventory.py                    # Full rebuild from a ManaPool export (rare)
├── tools/                 # Occasional reports: python -m tools.<module>
├── archive/               # Finished one-time code, kept for reference (not maintained)
├── tests/                 # python -m unittest discover -s tests
├── config/                # repricer.toml, set_box_map.csv (set -> box)
└── exports/ audits/ backups/   # Generated data, git-ignored (backups/*.dump only)
```

Run every command from the repository root.

---

## Physical Storage Model

- Storage boxes are labelled **M1–M19**. `config/set_box_map.csv` maps each set to its box.
- Each set lives in one box, split into append-only **sections** (`Sec-1`, `Sec-2`, …). Each intake batch becomes the next section, sorted in shelf order and indexed `1..N`. Shelf order ignores case and accents (Éomer files under E), puts leading symbols first, and lists same-name printings by collector number (regular printing before showcase variants).
- **Tokens** (Scryfall layout `token`, `double_faced_token` or `emblem`) form a shelf set called `TOKENS`, currently in box **M19**. Each intake batch's tokens, from any set, become the next append-only chunk (`TOKEN-1`, `TOKEN-2`, …). Tokens keep their real set code.
- Box **M19** also holds `PROMOS` (standalone promo sets).

---

## Key Features

1. **Interactive Curses TUI (`tui.py`)**:
   - Keyboard-driven terminal console with real-time database and API indicators.
   - Menu: batch intake, push inventory to ManaPool, pick sheet from live API orders, pick sheet from a picklist CSV, browse inventory, box shuffle, diagnostics, Scryfall mirror refresh, personal collection pulls, [R] reprice, and [P] price pins.

2. **ManaPool Batch Ingestion (`station/csv_pipeline.py`)**:
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

4. **Order Pull List Optimizer (`station/pull_list_optimizer.py`)**:
   - Ingests ManaPool picklists either from **live ManaPool API** (`GET /seller/orders?is_unfulfilled=true`) or local CSV exports (`picklist-*.csv`).
   - Resolves order lines through the Scryfall mirror (same rules as intake) and physical coordinates: `Box -> Set -> Section -> Index` (token chunks: `Box -> Section -> Index`).
   - **Zero-Backtracking Path Optimization**: Sorts the picking sequence sequentially along the warehouse walking route.
   - Generates printable pick sheets (`exports/pick_sheet_*.txt`) with `[ ]` checkboxes for clipboard picking.
   - Supports stock deduction (`--commit`) directly updating PostgreSQL.

5. **ManaPool API v1 Integration (`station/manapool_client.py`)**:
   - `python -m station.manapool_client` checks credentials with a read-only `GET /account`.
   - `python -m station.manapool_client preview-push [--set MOM ...] [--baseline <csv>]` is a dry run: it builds the exact payload a push would send, optionally compares it with a ManaPool inventory export, and writes a comparison CSV plus the JSON payload. Nothing is sent. Use a fresh export as the baseline, or use `python -m station.audit push` / `python -m station.repricer sync`, which compare against live ManaPool.
   - `python -m station.manapool_client push-changed --set MOM [--set ...] --baseline <csv> [--send]` pushes the quantity (price left unchanged) of products whose quantity differs from the baseline (or that are new), then reads each one back and reports mismatches.
   - `python -m station.manapool_client push-one --product <uuid> [--send]` reads the live listing, shows the payload, and with `--send` pushes just that product's database total and price and reads it back. Use it to list an in-stock product that has no ManaPool listing yet.
   - Endpoints and payloads follow ManaPool's published spec: https://manapool.com/api/docs/v1/openapi.json
   - Direct batch inventory updates via `POST /seller/inventory/product` (up to 2,000 items per request, retried when rate-limited). Quantity syncs send `price_cents: null` and price pushes send `quantity: null`, so each leaves the other field as it is on ManaPool.
   - Automated unfulfilled order retrieval and item parsing.
   - Graceful fallback to offline CSV mode when API credentials are not set.

6. **Personal Collection Pulls (`station/collection_pull.py`)**:
   - Search in-stock cards by name (case/accent-insensitive), or pull one card or a whole decklist (`4 Lightning Bolt`, `1 Name (SET) 123`, `*F*` for foil).
   - Takes the cheapest in-stock printing first (oldest section first), prints a pull sheet in walk order (`exports/personal_pull_*.txt`), and lists anything missing or short. A row picked from a TUI search pulls exactly that copy (condition and location).
   - Holds back copies needed by unfulfilled ManaPool orders. If open orders cannot be fully checked (API error, or no API credentials), nothing is pulled unless `--force` is given; the TUI asks before forcing.
   - On confirmation, deducts the cards and pushes the new absolute quantities (price left unchanged) for just those products to ManaPool; without API credentials (or if the push fails) it writes `exports/manapool_sync_*.csv` to upload by hand.

7. **Repricer (`station/repricer.py`, settings in `config/repricer.toml`)**:
   - Prices every in-stock listing at max(lowest NM listing, market) −1%, floor $0.15, from ManaPool's public price feed (one request per run).
   - Never undercuts itself: when the lowest NM listing equals our own price (it may be our listing), the price is held, or raised if the market anchor alone is higher. Products with no ManaPool listing are skipped.
   - Changes over both review limits (30% and $0.50) need your review; everything else is applied automatically. Risers (lowest listing far above market, supply draining, market climbing) are flagged in the report.
   - Refuses to push while live ManaPool prices differ from the database (`sync --apply` fixes that); pushes price only, then reads prices back to verify.
   - `snapshot` records daily price history (run nightly from cron, see below); pins hold or fix individual prices. TUI: [R] reprice, [P] pins.

8. **Scryfall Mirror (`station/scryfall_mirror.py`)**:
   - `python -m station.scryfall_mirror refresh` loads Scryfall's Default Cards bulk file (about a minute); `status` shows its size and age. Refresh before intaking a newly released set.

9. **Physical Audit (`station/audit.py`)**:
   - `generate` writes a walk-order count sheet (`audits/audit_<date>.txt`) and a fillable CSV covering listings where ManaPool and the database disagree (sampled per set, `--full-sets` counted in full; in-stock products with no ManaPool listing count as disagreeing), a random baseline of agreeing listings, and every listing worth $1+. Rows counted in the last 30 days are left out (`--skip-counted-days N` to change).
   - `apply <csv|xlsx>` imports the `counted` column, updates those rows, logs them to `audits/applied_counts.csv`, and writes a follow-up full-count sheet for any set whose sample had an overcount. A count is skipped if its row no longer holds the sheet's product at the sheet's box / section / index (e.g. inventory rebuilt or relocated since printing).
   - `push [--send] [--zero <csv>]` pushes audited database quantities (quantity only) to every ManaPool listing that differs, then reads them back; `--zero` sets dropped products (e.g. `audits/dropped_sets_*.csv`) to 0. In-stock products with no ManaPool listing are reported, not pushed (list them with `push-one`).
   - Conservative listing: listings not yet counted are held at ManaPool's lower quantity and recorded in `audits/needs_count.csv` (with the previous database quantity); `generate --needs-count [--sets CMR NEO]` prints a count sheet for them. All 828 such listings were counted by Oct 29, 2026 and the list was removed.
   - Oct 2026: SOI and BFZ were dropped from the database and ManaPool for a clean relist (`audits/dropped_sets_2026-10-21_SOI_BFZ.csv`).
   - `audits/` holds data, not code: it is git-ignored, so count sheets, `applied_counts.csv` and the dropped-sets list live only on the station's machine.
   - Oct 30, 2026: the counted database was declared final and dumped to `backups/pg_dump_locked_20261030-101257.dump`. `backups/pg_dump_pre_drop_20261021-104925.dump` is kept until SOI/BFZ are relisted. Dumps are git-ignored. Restore with `docker exec -i mtg_postgres pg_restore -U mtg_admin -d mtg_inventory --clean < backups/<file>.dump`.

10. **Rebuild, Reports & Archive**:
   - `python -m station.import_live_inventory --inventory-csv <export>`: fresh import of a ManaPool inventory export against the Scryfall mirror and Set/Box map. Previews by default; `--rebuild` drops and recreates the inventory tables atomically (take a `pg_dump` first).
   - `python -m tools.token_validation_report`: read-only report of tokens filed in set sections, non-tokens in token sections, and rows missing from the mirror.
   - `python -m tools.special_cases_report --inventory-csv <export>`: printable check sheet of unusual cards (combos, emblems, helper cards, promos, accent/symbol names, same-name printings, The List) at their planned locations.
   - `archive/audit_inventory.py`: one-time audit that assigned boxes to unmapped sets and produced the original token extraction walk sheet (finished).
   - `archive/hashing_engine.py`: early scanner work; not part of the current workflow.

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
python -m station.db   # runs schema.sql
python -m station.scryfall_mirror refresh
```

### 3. Configure Environment (`.env`)
```ini
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mtg_inventory
DB_USER=mtg_admin
DB_PASS=password
SCRYFALL_USER_AGENT=mtg_scanner/1.0

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
python -m station.csv_pipeline ingest multiple_sets_singles_<timestamp>.csv

# Force a specific box
python -m station.csv_pipeline ingest batch.csv --box M18

# Ingest and push directly to ManaPool API
python -m station.csv_pipeline ingest batch.csv --push-api
```

### Relocate Set Between Boxes (Box Shuffle)
```bash
# Move set ZNR from M3 to M4 when M3 is full
python -m station.csv_pipeline relocate --source M3 --target M4 --set ZNR

# Move a single section
python -m station.csv_pipeline relocate --source M3 --target M4 --set ZNR --section Sec-1

# Move every token chunk to a new box
python -m station.csv_pipeline relocate --source M19 --target M20 --set TOKENS
```

### Pull Cards for a Personal Collection
```bash
python -m station.collection_pull search "lightning bolt"
python -m station.collection_pull pull "Lightning Bolt" --qty 4
python -m station.collection_pull pull --decklist mydeck.txt
python -m station.collection_pull pull --decklist mydeck.txt --force   # pull even if open orders cannot be checked
```

### Reprice
```bash
python -m station.repricer run --preview     # plan and report only
python -m station.repricer run               # drift check, review, confirm, push, verify
python -m station.repricer sync [--apply]    # compare / copy live ManaPool prices into the database
python -m station.repricer snapshot          # record today's price history (nightly via cron)
python -m station.repricer pin --set CMR --number 123 [--foil] [--price 2.50]
```

Nightly price history (crontab):
```
0 3 * * * cd /home/bbrown/mtg_scanner && venv/bin/python -m station.repricer snapshot >> exports/snapshot.log 2>&1
```

### Physical Audit
```bash
python -m station.audit generate [--full-sets SOI] [--skip-counted-days 30]
python -m station.audit generate --needs-count [--sets CMR NEO]
python -m station.audit apply audits/audit_2026-10-19.csv
python -m station.audit push             # dry run against live ManaPool
python -m station.audit push --send      # push differing quantities, then read back
```

### Browse Physical Inventory
```bash
python -m station.csv_pipeline list
python -m station.csv_pipeline list --box M1
```

### Run Order Fulfillment Picklist
```bash
# Preview optimized pick sheet from CSV (no stock deduction)
python -m station.pull_list_optimizer picklist-<date>.csv

# Process CSV and deduct pulled cards from PostgreSQL inventory
python -m station.pull_list_optimizer picklist-<date>.csv --commit

# Pull unfulfilled orders directly from ManaPool API and deduct
python -m station.pull_list_optimizer --api --commit
```

---

## Running Automated Tests
```bash
python -m unittest discover -s tests -v
```
The integration tests run against a separate `mtg_inventory_test` database (override with `TEST_DB_NAME`), which they create and reset themselves; the live database is never touched.

---

## Known Limitations
- **ManaPool combo names:** a combo token's displayed name comes from Scryfall's face order, which can differ from ManaPool's product name (e.g. ZNR `5-10`).
