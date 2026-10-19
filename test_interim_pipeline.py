"""Automated test suite for MTG Interim Inventory Management Pipeline.

Runs against a separate database (TEST_DB_NAME, default 'mtg_inventory_test') that is
created if missing and rebuilt from schema.sql on every run; the live inventory database
is never touched. Every mock CSV row is also seeded into the test scryfall_cards table.

Tests:
1. Database schema initialization & connection.
2. Section auto-allocation (Sec-1, Sec-2, etc.).
3. Strict alphabetical sorting by card name & sequential 1..N index assignment.
4. ManaPool upload CSV generation.
5. Box reallocation (Box Shuffle when space runs out).
6. Fulfillment picklist path optimization (Box -> Set -> Section -> Index).
7. Stock quantity decrement upon pick commitment.
"""

import csv
import os
import shutil
import unittest
import psycopg2
from psycopg2 import extras

import collection_pull
import db
import csv_pipeline
import manapool_client
import pull_list_optimizer
from unittest.mock import MagicMock, patch


class TestMTGInterimPipeline(unittest.TestCase):
    """Integration test suite for the interim inventory pipeline."""

    @classmethod
    def setUpClass(cls):
        """Points db at a dedicated test database, rebuilds its schema, creates test directory."""
        cls.test_dir = "test_exports"
        os.makedirs(cls.test_dir, exist_ok=True)
        cls.live_db_name = db.DB_NAME
        test_db_name = os.getenv("TEST_DB_NAME", "mtg_inventory_test")
        if test_db_name == cls.live_db_name:
            raise RuntimeError("TEST_DB_NAME must differ from the live DB_NAME.")

        admin = psycopg2.connect(host=db.DB_HOST, port=db.DB_PORT, dbname="postgres", user=db.DB_USER, password=db.DB_PASS)
        admin.autocommit = True
        with admin.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (test_db_name,))
            if not cur.fetchone():
                cur.execute(f'CREATE DATABASE "{test_db_name}";')
        admin.close()

        db.DB_NAME = test_db_name
        with db.get_db_cursor() as cur:
            cur.execute("DROP TABLE IF EXISTS price_history, inventory, cards, scryfall_cards CASCADE;")
        db.init_db()

    @classmethod
    def tearDownClass(cls):
        """Restores the live database name and cleans up test exports directory."""
        db.DB_NAME = cls.live_db_name
        if os.path.exists(cls.test_dir):
            shutil.rmtree(cls.test_dir)

    def setUp(self):
        """Empties the test database's inventory and catalog before each test."""
        with db.get_db_cursor() as cur:
            cur.execute("TRUNCATE inventory, price_history, scryfall_cards RESTART IDENTITY;")
        # Use a test Set/Box map (no TOKENS row) instead of the real one.
        self.write_set_box_map({})

    def tearDown(self):
        csv_pipeline.SET_BOX_MAP_CSV = self._real_map

    def write_set_box_map(self, mapping: dict) -> None:
        """Points csv_pipeline at a test Set/Box map containing only `mapping`."""
        path = os.path.join(self.test_dir, "set_box_map.csv")
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["set_label", "Box"])
            for code, box in mapping.items():
                writer.writerow([f"({code}) Test", box])
        if not hasattr(self, "_real_map"):
            self._real_map = csv_pipeline.SET_BOX_MAP_CSV
        csv_pipeline.SET_BOX_MAP_CSV = path

    def seed_scryfall_cards(self, rows: list) -> None:
        """Inserts a test scryfall_cards row for each mock ManaPool row (layout from 'layout', default normal)."""
        values = [
            (
                f"sf-{r['set'].lower()}-{r['number'].lower()}",
                r["name"],
                r["set"].upper(),
                "token" if r.get("layout") in ("token", "emblem", "double_faced_token") else "expansion",
                r["number"],
                "common",
                r.get("layout", "normal"),
                "Card",
                None,
                extras.Json({}),
            )
            for r in rows
            if not r.get("_skip_seed")
        ]
        if not values:
            return
        with db.get_db_cursor() as cur:
            extras.execute_values(cur, """
                INSERT INTO scryfall_cards (scryfall_id, name, set_code, set_type, collector_number,
                                            rarity, layout, type_line, image_uri, raw)
                VALUES %s ON CONFLICT (scryfall_id) DO NOTHING;
            """, values)

    def create_mock_manapool_csv(self, filename: str, rows: list) -> str:
        """Helper to create a mock ManaPool set CSV."""
        self.seed_scryfall_cards(rows)
        path = os.path.join(self.test_dir, filename)
        fieldnames = [
            "product_type", "product_id", "name", "set", "number", "language",
            "finish", "condition", "market_low", "market_price", "market_price_foil",
            "price", "quantity"
        ]
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        return path

    def test_01_ingest_alphabetical_indexing(self):
        """Verifies that cards are sorted alphabetically by name and assigned indices 1..N."""
        # Provide cards out of alphabetical order: Z, A, M
        mock_cards = [
            {
                "product_type": "mtg_single",
                "product_id": "00000000-0000-0000-0000-000000000001",
                "name": "Zulaport Cutthroat",
                "set": "TEST",
                "number": "100",
                "language": "EN",
                "finish": "NF",
                "condition": "NM",
                "market_low": "0.50",
                "market_price": "1.00",
                "market_price_foil": "",
                "price": "1.00",
                "quantity": "2",
            },
            {
                "product_type": "mtg_single",
                "product_id": "00000000-0000-0000-0000-000000000002",
                "name": "Avacyn, Angel of Hope",
                "set": "TEST",
                "number": "1",
                "language": "EN",
                "finish": "NF",
                "condition": "NM",
                "market_low": "20.00",
                "market_price": "25.00",
                "market_price_foil": "",
                "price": "25.00",
                "quantity": "1",
            },
            {
                "product_type": "mtg_single",
                "product_id": "00000000-0000-0000-0000-000000000003",
                "name": "Mana Crypt",
                "set": "TEST",
                "number": "50",
                "language": "EN",
                "finish": "FO",
                "condition": "NM",
                "market_low": "100.00",
                "market_price": "120.00",
                "market_price_foil": "150.00",
                "price": "150.00",
                "quantity": "1",
            },
            {
                # Row with quantity = 0 should be skipped
                "product_type": "mtg_single",
                "product_id": "00000000-0000-0000-0000-000000000004",
                "name": "Zero Quantity Card",
                "set": "TEST",
                "number": "999",
                "language": "EN",
                "finish": "NF",
                "condition": "NM",
                "market_low": "0.10",
                "market_price": "0.10",
                "market_price_foil": "",
                "price": "0.10",
                "quantity": "0",
            },
        ]

        csv_file = self.create_mock_manapool_csv("batch1.csv", mock_cards)
        res = csv_pipeline.ingest_csv_batch(
            file_path=csv_file,
            box_label="TestBox-1",
            output_dir=self.test_dir,
        )

        self.assertEqual(res["status"], "success")
        self.assertEqual(res["total_lines"], 3)  # 3 positive quantity cards
        self.assertEqual(res["total_cards"], 4)  # 2 + 1 + 1

        # Query database to verify alphabetical sorting and index assignment
        query = """
            SELECT i.index_num, c.name, i.section_label, i.quantity, i.is_foil
            FROM inventory i
            JOIN scryfall_cards c ON i.scryfall_id = c.scryfall_id
            WHERE i.box_label = 'TestBox-1' AND LOWER(c.set_code) = 'test'
            ORDER BY i.index_num;
        """
        with db.get_db_cursor(commit=False) as cur:
            cur.execute(query)
            rows = cur.fetchall()

        self.assertEqual(len(rows), 3)
        # 1. Avacyn (Index 1)
        self.assertEqual(rows[0]["index_num"], 1)
        self.assertEqual(rows[0]["name"], "Avacyn, Angel of Hope")
        self.assertEqual(rows[0]["section_label"], "Sec-1")

        # 2. Mana Crypt (Index 2)
        self.assertEqual(rows[1]["index_num"], 2)
        self.assertEqual(rows[1]["name"], "Mana Crypt")
        self.assertTrue(rows[1]["is_foil"])

        # 3. Zulaport Cutthroat (Index 3)
        self.assertEqual(rows[2]["index_num"], 3)
        self.assertEqual(rows[2]["name"], "Zulaport Cutthroat")
        self.assertEqual(rows[2]["quantity"], 2)

        # Verify the ManaPool upload CSV was created
        self.assertTrue(len(res["upload_csvs"]) > 0)
        self.assertTrue(os.path.isfile(res["upload_csvs"][0]))

    def test_02_auto_section_allocation(self):
        """Verifies that subsequent batches in the same box auto-allocate Sec-2, Sec-3."""
        mock_cards = [{
            "product_type": "mtg_single",
            "product_id": "00000000-0000-0000-0000-000000000010",
            "name": "Lightning Bolt",
            "set": "TEST",
            "number": "20",
            "language": "EN",
            "finish": "NF",
            "condition": "NM",
            "market_low": "1.00",
            "market_price": "2.00",
            "market_price_foil": "",
            "price": "2.00",
            "quantity": "3",
        }]

        # Batch 1 -> Sec-1
        csv1 = self.create_mock_manapool_csv("sec1.csv", mock_cards)
        csv_pipeline.ingest_csv_batch(csv1, "TestBox-1", output_dir=self.test_dir)

        # Batch 2 -> Sec-2
        csv2 = self.create_mock_manapool_csv("sec2.csv", mock_cards)
        res2 = csv_pipeline.ingest_csv_batch(csv2, "TestBox-1", output_dir=self.test_dir)

        query = """
            SELECT DISTINCT section_label 
            FROM inventory i
            JOIN scryfall_cards c ON i.scryfall_id = c.scryfall_id
            WHERE i.box_label = 'TestBox-1' AND LOWER(c.set_code) = 'test'
            ORDER BY section_label;
        """
        with db.get_db_cursor(commit=False) as cur:
            cur.execute(query)
            sections = [r["section_label"] for r in cur.fetchall()]

        self.assertIn("Sec-1", sections)
        self.assertIn("Sec-2", sections)

    def test_03_box_shuffle_relocation(self):
        """Verifies the box relocation function when a box runs out of room."""
        mock_cards = [{
            "product_type": "mtg_single",
            "product_id": "00000000-0000-0000-0000-000000000020",
            "name": "Sol Ring",
            "set": "TEST",
            "number": "30",
            "language": "EN",
            "finish": "NF",
            "condition": "NM",
            "market_low": "1.00",
            "market_price": "1.50",
            "market_price_foil": "",
            "price": "1.50",
            "quantity": "5",
        }]

        csv_file = self.create_mock_manapool_csv("shuffle.csv", mock_cards)
        csv_pipeline.ingest_csv_batch(csv_file, "TestBox-1", output_dir=self.test_dir)

        # Relocate TEST set from TestBox-1 to TestBox-2
        res = csv_pipeline.relocate_box(
            source_box="TestBox-1",
            target_box="TestBox-2",
            set_code="TEST",
        )

        self.assertEqual(res["status"], "success")
        self.assertGreater(res["updated_rows"], 0)

        # Verify database shows TestBox-2
        boxes = db.get_distinct_boxes()
        self.assertIn("TestBox-2", boxes)

        sets_in_box2 = db.get_sets_in_box("TestBox-2")
        set_codes = [s["set_code"].upper() for s in sets_in_box2]
        self.assertIn("TEST", set_codes)

        # Verify old box no longer has TEST set
        sets_in_box1 = db.get_sets_in_box("TestBox-1")
        test_in_box1 = any(s["set_code"].upper() == "TEST" for s in sets_in_box1)
        self.assertFalse(test_in_box1)

    def test_04_picklist_fulfillment_and_stock_deduction(self):
        """Verifies traversal route optimization (Box -> Set -> Sec -> Index) and stock deduction."""
        # 1. Ingest cards into Box A and Box B
        box_a_cards = [
            {
                "product_type": "mtg_single",
                "product_id": "00000000-0000-0000-0000-000000000031",
                "name": "Island",
                "set": "TEST",
                "number": "250",
                "language": "EN",
                "finish": "NF",
                "condition": "NM",
                "price": "0.25",
                "quantity": "4",
            },
            {
                "product_type": "mtg_single",
                "product_id": "00000000-0000-0000-0000-000000000032",
                "name": "Counterspell",
                "set": "TEST",
                "number": "55",
                "language": "EN",
                "finish": "NF",
                "condition": "NM",
                "price": "1.50",
                "quantity": "2",
            },
        ]
        box_b_cards = [
            {
                "product_type": "mtg_single",
                "product_id": "00000000-0000-0000-0000-000000000033",
                "name": "Dark Ritual",
                "set": "TEST",
                "number": "70",
                "language": "EN",
                "finish": "NF",
                "condition": "NM",
                "price": "2.00",
                "quantity": "3",
            }
        ]

        csv_a = self.create_mock_manapool_csv("box_a.csv", box_a_cards)
        csv_b = self.create_mock_manapool_csv("box_b.csv", box_b_cards)
        csv_pipeline.ingest_csv_batch(csv_a, "TestBox-A", output_dir=self.test_dir)
        csv_pipeline.ingest_csv_batch(csv_b, "TestBox-B", output_dir=self.test_dir)

        # 2. Create picklist matching order
        picklist_path = os.path.join(self.test_dir, "test_picklist.csv")
        with open(picklist_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["Qty", "Description", "Set", "Condition", "Finish", "Lang", "Number"])
            # Dark Ritual is in Box B; Island & Counterspell are in Box A
            # Even if Dark Ritual is listed first in order, traversal MUST sort Box A before Box B!
            writer.writerow(["1", "Dark Ritual", "TEST", "NM", "NF", "EN", "70"])
            writer.writerow(["2", "Island", "TEST", "NM", "NF", "EN", "250"])
            writer.writerow(["1", "Counterspell", "TEST", "NM", "NF", "EN", "55"])

        # 3. Process pick routine with --commit
        res = pull_list_optimizer.process_pick_routine(
            source_type="csv",
            csv_file=picklist_path,
            commit=True,
            output_dir=self.test_dir,
        )

        self.assertEqual(res["status"], "success")
        self.assertEqual(res["total_allocated"], 3)
        self.assertEqual(res["total_cards"], 4)  # 1 + 2 + 1
        self.assertEqual(res["unallocated_count"], 0)

        # 4. Verify traversal order: TestBox-A items MUST precede TestBox-B items
        allocated = res["allocated_items"]
        self.assertEqual(allocated[0]["box_label"], "TestBox-A")
        self.assertEqual(allocated[1]["box_label"], "TestBox-A")
        self.assertEqual(allocated[2]["box_label"], "TestBox-B")

        # In TestBox-A, Counterspell (Index 1) precedes Island (Index 2)
        self.assertEqual(allocated[0]["name"], "Counterspell")
        self.assertEqual(allocated[1]["name"], "Island")
        self.assertEqual(allocated[2]["name"], "Dark Ritual")

        # 5. Verify stock quantities decremented in database
        query = """
            SELECT c.name, i.quantity
            FROM inventory i
            JOIN scryfall_cards c ON i.scryfall_id = c.scryfall_id
            WHERE i.box_label IN ('TestBox-A', 'TestBox-B') AND LOWER(c.set_code) = 'test'
            ORDER BY c.name;
        """
        with db.get_db_cursor(commit=False) as cur:
            cur.execute(query)
            stock_after = {r["name"]: r["quantity"] for r in cur.fetchall()}

        # Counterspell was 2, pulled 1 -> now 1
        self.assertEqual(stock_after["Counterspell"], 1)
        # Dark Ritual was 3, pulled 1 -> now 2
        self.assertEqual(stock_after["Dark Ritual"], 2)
        # Island was 4, pulled 2 -> now 2
        self.assertEqual(stock_after["Island"], 2)

    def test_05_manapool_api_cumulative_quantity_aggregation(self):
        """Verifies that ManaPool API batch push sends CUMULATIVE in-stock totals across all boxes,

        not just the single batch quantity (which would overwrite/wipe existing stock).
        """
        test_product_id = "00000000-0000-0000-0000-000000000099"
        # Batch 1: Ingest 3 copies of card into TestBox-1
        batch_1_cards = [{
            "product_type": "mtg_single",
            "product_id": test_product_id,
            "name": "Serum Visions",
            "set": "TEST",
            "number": "99",
            "language": "EN",
            "finish": "NF",
            "condition": "NM",
            "price": "1.25",
            "quantity": "3",
        }]
        csv1 = self.create_mock_manapool_csv("api_batch1.csv", batch_1_cards)
        csv_pipeline.ingest_csv_batch(csv1, "TestBox-1", output_dir=self.test_dir)

        # Batch 2: Ingest 2 MORE copies of the same card into TestBox-2
        batch_2_cards = [{
            "product_type": "mtg_single",
            "product_id": test_product_id,
            "name": "Serum Visions",
            "set": "TEST",
            "number": "99",
            "language": "EN",
            "finish": "NF",
            "condition": "NM",
            "price": "1.25",
            "quantity": "2",
        }]
        csv2 = self.create_mock_manapool_csv("api_batch2.csv", batch_2_cards)
        csv_pipeline.ingest_csv_batch(csv2, "TestBox-2", output_dir=self.test_dir)

        # Verify DB totals: TestBox-1 has 3, TestBox-2 has 2 -> Total should be 5
        db_totals = db.get_total_quantities_by_product_ids([test_product_id])
        self.assertEqual(db_totals[test_product_id], 5)

        # Mock the ManaPool API post call to inspect the payload sent
        with patch.object(manapool_client, "is_configured", return_value=True), \
             patch("requests.post") as mock_post:

            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_post.return_value = mock_response

            # When pushing the new batch (which had quantity: 2),
            # push_inventory_batch should aggregate with the DB and send quantity: 5!
            updated_count, errors = manapool_client.push_inventory_batch(
                items=batch_2_cards,
                aggregate_with_db=True,
            )

            self.assertEqual(updated_count, 1)
            self.assertEqual(len(errors), 0)

            # Inspect the JSON payload sent to ManaPool
            mock_post.assert_called_once()
            called_payload = mock_post.call_args[1]["json"]
            self.assertEqual(len(called_payload), 1)

            entry = called_payload[0]
            self.assertEqual(entry["product_id"], test_product_id)
            # CRITICAL CHECK: Quantity sent MUST be 5 (cumulative sum), NOT 2 (batch amount)
            self.assertEqual(entry["quantity"], 5)
            self.assertEqual(entry["price_cents"], 125)

    def test_06_auto_box_resolution_and_multi_set_intake(self):
        """Verifies multi-set CSV intake auto-resolves each set's box and allocates Sec-2."""
        # 1. Establish initial baseline: TESTA in TestBox-1 (Sec-1), TESTB in TestBox-2 (Sec-1)
        init_cards_a = [{
            "product_type": "mtg_single",
            "product_id": "00000000-0000-0000-0000-000000000101",
            "name": "Alpha Card A1",
            "set": "TESTA",
            "number": "1",
            "language": "EN",
            "finish": "NF",
            "condition": "NM",
            "price": "1.00",
            "quantity": "2",
        }]
        init_cards_b = [{
            "product_type": "mtg_single",
            "product_id": "00000000-0000-0000-0000-000000000102",
            "name": "Beta Card B1",
            "set": "TESTB",
            "number": "1",
            "language": "EN",
            "finish": "NF",
            "condition": "NM",
            "price": "2.00",
            "quantity": "1",
        }]
        csv_init_a = self.create_mock_manapool_csv("init_a.csv", init_cards_a)
        csv_init_b = self.create_mock_manapool_csv("init_b.csv", init_cards_b)
        csv_pipeline.ingest_csv_batch(csv_init_a, box_label="TestBox-1", output_dir=self.test_dir)
        csv_pipeline.ingest_csv_batch(csv_init_b, box_label="TestBox-2", output_dir=self.test_dir)

        # 2. Prepare multi-set CSV with new cards for BOTH TESTA and TESTB
        multi_cards = [
            {
                "product_type": "mtg_single",
                "product_id": "00000000-0000-0000-0000-000000000103",
                "name": "Zeta Card A2",
                "set": "TESTA",
                "number": "2",
                "language": "EN",
                "finish": "NF",
                "condition": "NM",
                "price": "0.50",
                "quantity": "3",
            },
            {
                "product_type": "mtg_single",
                "product_id": "00000000-0000-0000-0000-000000000104",
                "name": "Bravo Card A3",
                "set": "TESTA",
                "number": "3",
                "language": "EN",
                "finish": "NF",
                "condition": "NM",
                "price": "0.75",
                "quantity": "1",
            },
            {
                "product_type": "mtg_single",
                "product_id": "00000000-0000-0000-0000-000000000105",
                "name": "Delta Card B2",
                "set": "TESTB",
                "number": "2",
                "language": "EN",
                "finish": "NF",
                "condition": "NM",
                "price": "3.00",
                "quantity": "2",
            },
        ]
        multi_csv = self.create_mock_manapool_csv("multi_batch.csv", multi_cards)

        # 3. Ingest multi-set CSV WITHOUT providing box_label
        res = csv_pipeline.ingest_csv_batch(
            file_path=multi_csv,
            box_label=None,  # Auto-resolve!
            output_dir=self.test_dir,
        )

        self.assertEqual(res["status"], "success")
        self.assertEqual(res["set_boxes"]["TESTA"], "TestBox-1")
        self.assertEqual(res["set_boxes"]["TESTB"], "TestBox-2")

        # 4. Verify TESTA was assigned to Sec-2 in TestBox-1, alphabetized
        query_a = """
            SELECT i.section_label, i.index_num, c.name, i.box_label
            FROM inventory i
            JOIN scryfall_cards c ON i.scryfall_id = c.scryfall_id
            WHERE LOWER(c.set_code) = 'testa' AND i.section_label = 'Sec-2'
            ORDER BY i.index_num;
        """
        with db.get_db_cursor(commit=False) as cur:
            cur.execute(query_a)
            rows_a = cur.fetchall()

        self.assertEqual(len(rows_a), 2)
        # Bravo precedes Zeta
        self.assertEqual(rows_a[0]["name"], "Bravo Card A3")
        self.assertEqual(rows_a[0]["index_num"], 1)
        self.assertEqual(rows_a[0]["box_label"], "TestBox-1")

        self.assertEqual(rows_a[1]["name"], "Zeta Card A2")
        self.assertEqual(rows_a[1]["index_num"], 2)
        self.assertEqual(rows_a[1]["box_label"], "TestBox-1")

        # 5. Verify TESTB was assigned to Sec-2 in TestBox-2
        query_b = """
            SELECT i.section_label, i.index_num, c.name, i.box_label
            FROM inventory i
            JOIN scryfall_cards c ON i.scryfall_id = c.scryfall_id
            WHERE LOWER(c.set_code) = 'testb' AND i.section_label = 'Sec-2'
            ORDER BY i.index_num;
        """
        with db.get_db_cursor(commit=False) as cur:
            cur.execute(query_b)
            rows_b = cur.fetchall()

        self.assertEqual(len(rows_b), 1)
        self.assertEqual(rows_b[0]["name"], "Delta Card B2")
        self.assertEqual(rows_b[0]["index_num"], 1)
        self.assertEqual(rows_b[0]["box_label"], "TestBox-2")

    def test_07_unmapped_new_set_callback_and_db_persistence(self):
        """Verifies unmapped new set queries callback, assigns Sec-1, and future batches resolve via DB."""
        new_card = [{
            "product_type": "mtg_single",
            "product_id": "00000000-0000-0000-0000-000000000106",
            "name": "Hobbit Wanderer",
            "set": "TESTHOB",
            "number": "1",
            "language": "EN",
            "finish": "NF",
            "condition": "NM",
            "price": "4.50",
            "quantity": "2",
        }]
        csv_file = self.create_mock_manapool_csv("testhob.csv", new_card)

        # Batch 1: Ingest brand new set with callback returning TestBox-5
        res = csv_pipeline.ingest_csv_batch(
            file_path=csv_file,
            box_label=None,
            output_dir=self.test_dir,
            prompt_callback=lambda s: "TestBox-5",
        )

        self.assertEqual(res["status"], "success")
        self.assertEqual(res["set_boxes"]["TESTHOB"], "TestBox-5")

        # Verify DB records
        query = """
            SELECT i.section_label, i.box_label, i.index_num
            FROM inventory i
            JOIN scryfall_cards c ON i.scryfall_id = c.scryfall_id
            WHERE LOWER(c.set_code) = 'testhob';
        """
        with db.get_db_cursor(commit=False) as cur:
            cur.execute(query)
            rows = cur.fetchall()

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["box_label"], "TestBox-5")
        self.assertEqual(rows[0]["section_label"], "Sec-1")
        self.assertEqual(rows[0]["index_num"], 1)

        # Batch 2: Subsequent intake of TESTHOB without any prompt callback or box_label
        # It must resolve TestBox-5 purely from PostgreSQL inventory and allocate Sec-2!
        second_card = [{
            "product_type": "mtg_single",
            "product_id": "00000000-0000-0000-0000-000000000107",
            "name": "Hobbit Tracker",
            "set": "TESTHOB",
            "number": "2",
            "language": "EN",
            "finish": "NF",
            "condition": "NM",
            "price": "2.50",
            "quantity": "1",
        }]
        csv_file2 = self.create_mock_manapool_csv("testhob2.csv", second_card)
        res2 = csv_pipeline.ingest_csv_batch(
            file_path=csv_file2,
            box_label=None,
            output_dir=self.test_dir,
            prompt_callback=None,  # No callback needed! Resolved purely from DB.
        )
        self.assertEqual(res2["status"], "success")
        self.assertEqual(res2["set_boxes"]["TESTHOB"], "TestBox-5")

        query_sec2 = """
            SELECT i.section_label, i.box_label, i.index_num
            FROM inventory i
            JOIN scryfall_cards c ON i.scryfall_id = c.scryfall_id
            WHERE LOWER(c.set_code) = 'testhob' AND i.section_label = 'Sec-2';
        """
        with db.get_db_cursor(commit=False) as cur:
            cur.execute(query_sec2)
            rows2 = cur.fetchall()

        self.assertEqual(len(rows2), 1)
        self.assertEqual(rows2[0]["box_label"], "TestBox-5")
        self.assertEqual(rows2[0]["section_label"], "Sec-2")
        self.assertEqual(rows2[0]["index_num"], 1)

    def test_08_pull_list_hierarchy_box_set_section_name(self):
        """Verifies pull list traversal strictly follows Box -> Set -> Section -> Name / Index."""
        cards_b1_a1 = [{
            "product_type": "mtg_single",
            "product_id": "00000000-0000-0000-0000-000000000201",
            "name": "Avacyn",
            "set": "TESTA",
            "number": "10",
            "language": "EN",
            "finish": "NF",
            "condition": "NM",
            "price": "1.00",
            "quantity": "1",
        }]
        cards_b1_a2 = [{
            "product_type": "mtg_single",
            "product_id": "00000000-0000-0000-0000-000000000202",
            "name": "Bravo",
            "set": "TESTA",
            "number": "20",
            "language": "EN",
            "finish": "NF",
            "condition": "NM",
            "price": "1.00",
            "quantity": "1",
        }]
        cards_b1_b1 = [{
            "product_type": "mtg_single",
            "product_id": "00000000-0000-0000-0000-000000000203",
            "name": "Beta",
            "set": "TESTB",
            "number": "30",
            "language": "EN",
            "finish": "NF",
            "condition": "NM",
            "price": "1.00",
            "quantity": "1",
        }]
        cards_b2_a1 = [{
            "product_type": "mtg_single",
            "product_id": "00000000-0000-0000-0000-000000000204",
            "name": "Angel",
            "set": "TESTA",
            "number": "40",
            "language": "EN",
            "finish": "NF",
            "condition": "NM",
            "price": "1.00",
            "quantity": "1",
        }]

        c1 = self.create_mock_manapool_csv("t8_b1_a1.csv", cards_b1_a1)
        c2 = self.create_mock_manapool_csv("t8_b1_a2.csv", cards_b1_a2)
        c3 = self.create_mock_manapool_csv("t8_b1_b1.csv", cards_b1_b1)
        c4 = self.create_mock_manapool_csv("t8_b2_a1.csv", cards_b2_a1)

        csv_pipeline.ingest_csv_batch(c1, box_label="TestBox-1", section_label="Sec-1", output_dir=self.test_dir)
        csv_pipeline.ingest_csv_batch(c2, box_label="TestBox-1", section_label="Sec-2", output_dir=self.test_dir)
        csv_pipeline.ingest_csv_batch(c3, box_label="TestBox-1", section_label="Sec-1", output_dir=self.test_dir)
        csv_pipeline.ingest_csv_batch(c4, box_label="TestBox-2", section_label="Sec-1", output_dir=self.test_dir)

        # Create picklist requesting all 4 in inverted order
        pick_file = os.path.join(self.test_dir, "t8_pick.csv")
        with open(pick_file, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(["Qty", "Description", "Set", "Condition", "Finish", "Lang", "Number"])
            w.writerow(["1", "Angel", "TESTA", "NM", "NF", "EN", "40"])  # TestBox-2
            w.writerow(["1", "Beta", "TESTB", "NM", "NF", "EN", "30"])   # TestBox-1, TESTB
            w.writerow(["1", "Bravo", "TESTA", "NM", "NF", "EN", "20"])  # TestBox-1, TESTA, Sec-2
            w.writerow(["1", "Avacyn", "TESTA", "NM", "NF", "EN", "10"]) # TestBox-1, TESTA, Sec-1

        res = pull_list_optimizer.process_pick_routine(
            source_type="csv",
            csv_file=pick_file,
            commit=False,
            output_dir=self.test_dir,
        )

        self.assertEqual(res["status"], "success")
        allocated = res["allocated_items"]
        self.assertEqual(len(allocated), 4)

        # Expected traversal order strictly:
        # 1. TestBox-1, TESTA, Sec-1: Avacyn
        # 2. TestBox-1, TESTA, Sec-2: Bravo
        # 3. TestBox-1, TESTB, Sec-1: Beta
        # 4. TestBox-2, TESTA, Sec-1: Angel
        self.assertEqual(allocated[0]["name"], "Avacyn")
        self.assertEqual(allocated[0]["box_label"], "TestBox-1")
        self.assertEqual(allocated[0]["set_code"], "TESTA")
        self.assertEqual(allocated[0]["section_label"], "Sec-1")

        self.assertEqual(allocated[1]["name"], "Bravo")
        self.assertEqual(allocated[1]["box_label"], "TestBox-1")
        self.assertEqual(allocated[1]["set_code"], "TESTA")
        self.assertEqual(allocated[1]["section_label"], "Sec-2")

        self.assertEqual(allocated[2]["name"], "Beta")
        self.assertEqual(allocated[2]["box_label"], "TestBox-1")
        self.assertEqual(allocated[2]["set_code"], "TESTB")
        self.assertEqual(allocated[2]["section_label"], "Sec-1")

        self.assertEqual(allocated[3]["name"], "Angel")
        self.assertEqual(allocated[3]["box_label"], "TestBox-2")
        self.assertEqual(allocated[3]["set_code"], "TESTA")
        self.assertEqual(allocated[3]["section_label"], "Sec-1")


    def token_row(self, set_code: str, number: str, name: str, layout: str = "token", qty: str = "1", **extra) -> dict:
        """Builds a mock ManaPool single row (used for token tests)."""
        return {
            "product_type": "mtg_single",
            "product_id": f"pid-{set_code}-{number}",
            "name": name,
            "set": set_code,
            "number": number,
            "language": "EN",
            "finish": "NF",
            "condition": "NM",
            "market_low": "0.10",
            "market_price": "0.20",
            "market_price_foil": "",
            "price": "0.20",
            "quantity": qty,
            "layout": layout,
            **extra,
        }

    def section_rows(self, section: str) -> list:
        with db.get_db_cursor(commit=False) as cur:
            cur.execute("""
                SELECT i.box_label, i.index_num, c.name, c.set_code, i.back_scryfall_id
                FROM inventory i JOIN scryfall_cards c ON i.scryfall_id = c.scryfall_id
                WHERE i.section_label = %s ORDER BY i.index_num;
            """, (section,))
            return [dict(r) for r in cur.fetchall()]

    def test_09_tokens_form_append_only_chunks_in_token_box(self):
        """Tokens from every set in a batch become one TOKEN-n chunk; the next batch gets TOKEN-n+1."""
        # First token chunk: box comes from the Set/Box map's TOKENS row.
        self.write_set_box_map({"TOKENS": "TestBox-T"})
        seed = self.create_mock_manapool_csv("tok_seed.csv", [self.token_row("TSEED", "1", "Angel")])
        csv_pipeline.ingest_csv_batch(seed, output_dir=self.test_dir, prompt_callback=lambda s: "WrongBox")
        self.assertEqual(self.section_rows("TOKEN-1")[0]["box_label"], "TestBox-T")

        # After that the existing chunk decides the box, even if the map changes.
        self.write_set_box_map({"TOKENS": "WrongBox"})

        # Mixed batch: a regular card, tokens from two sets, an emblem from a non-token set,
        # and a ManaPool double-sided combo whose faces exist only as separate Scryfall cards.
        self.seed_scryfall_cards([
            self.token_row("TCMB", "8", "Beast"),
            self.token_row("TCMB", "11", "Spider"),
        ])
        batch = [
            self.token_row("TESTR", "5", "Regular Card", layout="normal"),
            self.token_row("TZED", "3", "Zombie"),
            self.token_row("TANT", "2", "Ant"),
            self.token_row("TESTD", "76", "Ob Nixilis Emblem", layout="emblem"),
            self.token_row("TCMB", "8-11", "Beast // Spider", _skip_seed=True),
        ]
        csv_file = self.create_mock_manapool_csv("tok_batch.csv", batch)
        res = csv_pipeline.ingest_csv_batch(
            csv_file, output_dir=self.test_dir, prompt_callback=lambda s: "TestBox-R",
        )

        self.assertEqual(res["status"], "success")
        self.assertEqual(res["set_boxes"]["TOKENS"], "TestBox-T")
        self.assertEqual(res["set_sections"]["TOKENS"], "TOKEN-2")
        self.assertEqual(res["set_sections"]["TESTR"], "Sec-1")

        chunk = self.section_rows("TOKEN-2")
        self.assertEqual([r["name"] for r in chunk], ["Ant", "Beast", "Ob Nixilis Emblem", "Zombie"])
        self.assertEqual([r["index_num"] for r in chunk], [1, 2, 3, 4])
        self.assertTrue(all(r["box_label"] == "TestBox-T" for r in chunk))
        self.assertIsNotNone(chunk[1]["back_scryfall_id"])  # combo keeps its back face
        self.assertEqual(self.section_rows("TOKEN-1")[0]["name"], "Angel")  # old chunk untouched

    def test_10_unresolved_rows_stop_intake(self):
        """A row missing from the Scryfall mirror aborts the whole batch and is listed."""
        rows = [
            self.token_row("TESTR", "5", "Regular Card", layout="normal"),
            self.token_row("TESTR", "999", "Typo Card", layout="normal", _skip_seed=True),
        ]
        csv_file = self.create_mock_manapool_csv("unresolved.csv", rows)
        res = csv_pipeline.ingest_csv_batch(csv_file, box_label="TestBox-1", output_dir=self.test_dir)

        self.assertEqual(res["status"], "unresolved")
        self.assertEqual([r["number"] for r in res["unresolved"]], ["999"])
        self.assertEqual(db.get_distinct_boxes(), [])

    def test_11_relocate_tokens_and_walk_token_chunk_by_index(self):
        """Relocating TOKENS moves every chunk; picks in a chunk follow its index, not set code."""
        for name, rows in [
            ("reloc1.csv", [self.token_row("TZZZ", "1", "Angel"), self.token_row("TAAA", "1", "Zombie")]),
            ("reloc2.csv", [self.token_row("TMMM", "1", "Bear")]),
        ]:
            csv_file = self.create_mock_manapool_csv(name, rows)
            csv_pipeline.ingest_csv_batch(csv_file, output_dir=self.test_dir, prompt_callback=lambda s: "TestBox-T")

        res = csv_pipeline.relocate_box(source_box="TestBox-T", target_box="TestBox-T2", set_code="TOKENS")
        self.assertEqual(res["updated_rows"], 3)
        self.assertEqual(db.get_token_box(), "TestBox-T2")
        self.assertEqual({r["section_label"] for r in db.get_inventory_summary("TestBox-T2")}, {"TOKEN-1", "TOKEN-2"})

        picks = [
            {"Qty": 1, "Description": "Zombie", "Set": "TAAA", "Number": "1", "is_foil": False},
            {"Qty": 1, "Description": "Bear", "Set": "TMMM", "Number": "1", "is_foil": False},
            {"Qty": 1, "Description": "Angel", "Set": "TZZZ", "Number": "1", "is_foil": False},
        ]
        allocated, unallocated = pull_list_optimizer.resolve_and_optimize_picks(picks)
        self.assertEqual(unallocated, [])
        self.assertEqual(
            [(p["section_label"], p["index_num"]) for p in allocated],
            [("TOKEN-1", 1), ("TOKEN-1", 2), ("TOKEN-2", 1)],
        )


    def test_12_personal_pull_decklist_reservations_and_sync(self):
        """Decklist pull takes the cheapest copies, holds open-order copies, deducts, and syncs totals."""
        rows = [
            {**self.token_row("TESTP", "1", "Lightning Bolt", layout="normal", qty="3"), "price": "0.50", "product_id": "pid-bolt-1"},
            {**self.token_row("TESTP", "2", "Lightning Bolt", layout="normal", qty="2"), "price": "0.25", "product_id": "pid-bolt-2"},
            {**self.token_row("TESTP", "3", "Counterspell", layout="normal", qty="1"), "price": "1.00", "product_id": "pid-cs"},
        ]
        csv_file = self.create_mock_manapool_csv("personal.csv", rows)
        csv_pipeline.ingest_csv_batch(csv_file, box_label="TestBox-1", output_dir=self.test_dir)

        reqs, unparsed = collection_pull.parse_decklist(["Deck", "3 Lightning Bolt", "1x Counterspell", "2 Missing Card", "???"])
        self.assertEqual(unparsed, ["???"])

        # Counterspell's only copy is on an open order -> held back.
        picks, short = collection_pull.plan_pulls(collection_pull.load_stock(), reqs, reserved={"pid-cs": 1})
        self.assertEqual([(p["collector_number"], p["qty_to_pull"]) for p in picks], [("2", 2), ("1", 1)])
        held = {s["request"]["name"]: s for s in short}
        self.assertTrue(held["Counterspell"]["held_for_orders"])
        self.assertFalse(held["Missing Card"]["found_any"])

        # Commit with the API mocked: ManaPool receives new absolute totals and prices.
        with patch.object(collection_pull, "reserved_by_open_orders", return_value=({"pid-cs": 1}, "checked")), \
             patch.object(manapool_client, "is_configured", return_value=True), \
             patch("requests.post") as mock_post:
            mock_post.return_value = MagicMock(status_code=200)
            res = collection_pull.run_pull(reqs, "deck.txt", confirm=lambda lines: True, output_dir=self.test_dir)

        self.assertTrue(res["committed"])
        self.assertEqual(res["sync"]["mode"], "api")
        payload = {e["product_id"]: e for e in mock_post.call_args[1]["json"]}
        self.assertEqual(payload["pid-bolt-2"]["quantity"], 0)  # sold out, row kept at 0
        self.assertEqual(payload["pid-bolt-1"]["quantity"], 2)
        self.assertEqual(payload["pid-bolt-1"]["price_cents"], 50)
        self.assertNotIn("pid-cs", payload)
        self.assertEqual(db.get_total_quantities_by_product_ids(["pid-bolt-2", "pid-cs"]), {"pid-bolt-2": 0, "pid-cs": 1})

    def test_13_personal_pull_without_api_writes_sync_csv(self):
        """Without ManaPool credentials the pull still deducts and writes a CSV of absolute quantities."""
        rows = [{**self.token_row("TESTP", "9", "Island", layout="normal", qty="5"), "product_id": "pid-island"}]
        csv_file = self.create_mock_manapool_csv("island.csv", rows)
        csv_pipeline.ingest_csv_batch(csv_file, box_label="TestBox-1", output_dir=self.test_dir)

        with patch.object(manapool_client, "is_configured", return_value=False):
            res = collection_pull.run_pull(
                [{"qty": 2, "name": "island", "set_code": None, "number": None, "foil": None}],
                "single", confirm=None, output_dir=self.test_dir,
            )
            declined = collection_pull.run_pull(
                [{"qty": 1, "name": "Island", "set_code": None, "number": None, "foil": None}],
                "single", confirm=lambda lines: False, output_dir=self.test_dir,
            )

        self.assertEqual(res["sync"]["mode"], "csv")
        self.assertIn("not configured", res["order_status"])
        with open(res["sync"]["csv_path"], encoding="utf-8") as f:
            synced = list(csv.DictReader(f))
        self.assertEqual([(r["product_id"], r["quantity"]) for r in synced], [("pid-island", "3")])
        self.assertFalse(declined["committed"])
        self.assertEqual(db.get_total_quantities_by_product_ids(["pid-island"]), {"pid-island": 3})


    def test_14_preview_push_sends_nothing_and_compares_baseline(self):
        """preview_push builds the real payload, compares it with a baseline export, and never POSTs."""
        rows = [
            {**self.token_row("TESTV", "1", "Kept Card", layout="normal", qty="2"), "price": "0.30", "product_id": "pid-kept"},
            {**self.token_row("TESTV", "2", "Grown Card", layout="normal", qty="3"), "price": "0.40", "product_id": "pid-grown"},
            {**self.token_row("TESTV", "3", "New Card", layout="normal", qty="1"), "price": "0.50", "product_id": "pid-new"},
        ]
        csv_file = self.create_mock_manapool_csv("preview.csv", rows)
        csv_pipeline.ingest_csv_batch(csv_file, box_label="TestBox-1", output_dir=self.test_dir)

        baseline = os.path.join(self.test_dir, "baseline.csv")
        with open(baseline, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(["product_id", "quantity", "price"])
            writer.writerow(["pid-kept", "2", "0.30"])
            writer.writerow(["pid-grown", "1", "0.40"])

        with patch("requests.post") as mock_post:
            res = manapool_client.preview_push(["TESTV"], baseline, self.test_dir)
        mock_post.assert_not_called()

        flags = {r["product_id"]: r["flags"] for r in res["rows"]}
        self.assertEqual(flags, {"pid-kept": "", "pid-grown": "QTY CHANGE", "pid-new": "NOT IN BASELINE"})
        self.assertEqual(res["counts"]["unchanged"], 1)
        with open(res["json_path"], encoding="utf-8") as f:
            import json
            payload = {e["product_id"]: e["quantity"] for e in json.load(f)}
        self.assertEqual(payload, {"pid-kept": 2, "pid-grown": 3, "pid-new": 1})


if __name__ == "__main__":
    unittest.main()

