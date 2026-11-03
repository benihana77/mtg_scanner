"""Pure unit tests for intake sheets and the additive ingest push (no database, no network)."""

import csv
import datetime
import os
import tempfile
import unittest
from unittest import mock

from station import intake_sheet
from station import manapool_client

CONFIG = {"pricing": {"adjustment_pct": -1.0, "floor": 0.15}}


def mirror_card(sid, name, number, layout="normal", rarity="common"):
    return {"scryfall_id": sid, "name": name, "collector_number": number, "rarity": rarity, "layout": layout}


def variant(pid, finish="NF", condition="NM", low=15, language="EN"):
    return {"product_type": "mtg_single", "product_id": pid, "condition_id": condition, "finish_id": finish,
            "language_id": language, "low_price": low, "available_quantity": 1 if low else 0}


def product(sid, name, number, variants, market=20, market_foil=None):
    return {"scryfall_ids": [sid], "name": name, "set_code": "SOI", "number": number,
            "price_market": market, "price_market_foil": market_foil, "variants": variants}


CARDS = [
    mirror_card("s1", "Zombie Apocalypse", "137"),
    mirror_card("s2", "Aim High", "193"),
    mirror_card("s3", "Clue", "T1", layout="token"),
    mirror_card("s4", "Promo Oddity", "300"),          # no ManaPool product
    mirror_card("s5", "Foil Only", "301"),             # no NM non-foil variant
    mirror_card("s6", "Éomer Test", "50"),
]
PRODUCTS = [
    product("s1", "Zombie Apocalypse", "137", [variant("p1", low=40), variant("p1f", "FO", low=90),
                                                variant("p1lp", condition="LP", low=10)], market=50, market_foil=100),
    product("s2", "Aim High", "193", [variant("p2", low=15)], market=23),
    product("s5", "Foil Only", "301", [variant("p5f", "FO", low=200)], market=None, market_foil=250),
    product("s6", "Éomer Test", "50", [variant("p6", low=0)], market=None),   # nobody lists it, no market
]


class TestSheetPrice(unittest.TestCase):
    def test_repricer_rule_and_floor(self):
        self.assertEqual(intake_sheet.sheet_price("p", 15, 23, None, {}, CONFIG), (23, ""))
        self.assertEqual(intake_sheet.sheet_price("p", None, None, None, {}, CONFIG), (15, "no data"))

    def test_pins_apply_to_products_we_already_list(self):
        self.assertEqual(intake_sheet.sheet_price("p", 15, 23, 99, {"p": 500}, CONFIG), (500, "pinned"))
        self.assertEqual(intake_sheet.sheet_price("p", 15, 23, 99, {"p": None}, CONFIG), (99, "pinned"))
        # A hold pin with nothing listed has nothing to hold: use the rule.
        self.assertEqual(intake_sheet.sheet_price("p", 15, 23, None, {"p": None}, CONFIG), (23, ""))

    def test_never_undercuts_our_own_lowest_listing(self):
        self.assertEqual(intake_sheet.sheet_price("p", 40, 30, 40, {}, CONFIG), (40, "own listing is the lowest"))
        self.assertEqual(intake_sheet.sheet_price("p", 40, 60, 40, {}, CONFIG), (59, "own listing is the lowest"))


class TestBuildRows(unittest.TestCase):
    def test_non_foil_sheet(self):
        res = intake_sheet.build_rows("SOI", CARDS, PRODUCTS, False, CONFIG, {}, {})
        rows = res["rows"]
        # NM non-foil only, shelf order (accent-insensitive name), tokens out.
        self.assertEqual([r["name"] for r in rows], ["Aim High", "Éomer Test", "Zombie Apocalypse"])
        self.assertTrue(all(r["finish"] == "NF" and r["condition"] == "NM" and r["quantity"] == "0" for r in rows))
        self.assertEqual(res["tokens"], 1)
        self.assertEqual([(m["number"], m["reason"]) for m in res["missing"]],
                         [("300", "no ManaPool product"), ("301", "no NM non-foil")])
        by_pid = {r["product_id"]: r for r in rows}
        self.assertEqual(by_pid["p1"]["price"], "0.50")          # max(40, 50) - 1% = 49.5 -> 50
        self.assertEqual(by_pid["p2"]["price"], "0.23")
        self.assertEqual(by_pid["p6"]["market_low"], "")         # low_price 0 = nobody lists it
        self.assertEqual(by_pid["p6"]["price"], "0.15")
        self.assertEqual(res["no_data"], 1)
        self.assertEqual(res["prefill"], {pid: r["price"] for pid, r in by_pid.items()})

    def test_foil_sheet_uses_foil_variant_and_market(self):
        res = intake_sheet.build_rows("SOI", CARDS, PRODUCTS, True, CONFIG, {}, {})
        by_pid = {r["product_id"]: r for r in res["rows"]}
        self.assertEqual(set(by_pid), {"p1f", "p5f"})
        self.assertEqual(by_pid["p1f"]["price"], "0.99")
        self.assertEqual(by_pid["p5f"]["price"], "2.48")
        self.assertEqual({m["number"] for m in res["missing"]}, {"193", "300", "50"})


class SheetDirTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        patcher = mock.patch.object(intake_sheet, "INTAKE_DIR", os.path.join(self.tmp.name, "intake"))
        patcher.start()
        self.addCleanup(patcher.stop)

    def create(self, foil=False):
        with mock.patch.object(intake_sheet.scryfall_mirror, "cards_in_set", return_value=CARDS), \
             mock.patch.object(intake_sheet.manapool_client, "fetch_single_products", return_value=("t", PRODUCTS)), \
             mock.patch.object(intake_sheet, "load_listed", return_value={}), \
             mock.patch.object(intake_sheet.repricer, "load_pins", return_value={}):
            return intake_sheet.create_sheet("soi", foil=foil, config=CONFIG)


class TestCreateSheet(SheetDirTest):
    def test_writes_website_export_columns_and_sidecar(self):
        res = self.create()
        self.assertEqual(res["status"], "created")
        self.assertRegex(os.path.basename(res["path"]), r"^intake_SOI_\d{4}-\d{2}-\d{2}\.csv$")
        with open(res["path"], newline="") as f:
            self.assertEqual(next(csv.reader(f)), intake_sheet.SHEET_COLUMNS)
        meta = intake_sheet.read_meta(res["path"])
        self.assertEqual(meta["finish"], "NF")
        self.assertEqual(len(meta["prefill"]), 3)

    def test_refuses_sets_missing_from_mirror_and_token_sets(self):
        with mock.patch.object(intake_sheet.scryfall_mirror, "cards_in_set", return_value=[]):
            self.assertEqual(intake_sheet.create_sheet("NEW", config=CONFIG)["status"], "not_in_mirror")
        with mock.patch.object(intake_sheet.scryfall_mirror, "cards_in_set",
                               return_value=[mirror_card("t", "Clue", "1", layout="token")]):
            self.assertEqual(intake_sheet.create_sheet("TSOI", config=CONFIG)["status"], "token_set")

    def test_names_unique_and_resume_matches_set_and_finish(self):
        first = self.create()["path"]
        second = self.create()["path"]
        self.assertTrue(second.endswith("-2.csv"))
        foil = self.create(foil=True)["path"]
        self.assertIn("_foil_", foil)
        self.assertIn(intake_sheet.find_unfinished("SOI", False), {first, second})
        self.assertEqual(intake_sheet.find_unfinished("SOI", True), foil)
        self.assertIsNone(intake_sheet.find_unfinished("BFZ", False))

    def test_retire_moves_sheet_and_sidecar_but_not_files_outside_intake(self):
        path = self.create()["path"]
        dest = intake_sheet.retire_sheet(path)
        self.assertEqual(os.path.dirname(dest), intake_sheet.ingested_dir())
        self.assertFalse(os.path.exists(path))
        self.assertIsNotNone(intake_sheet.read_meta(dest))
        self.assertEqual(intake_sheet.pending_sheets(), [])
        # A new sheet that day must not reuse the retired name.
        self.assertTrue(self.create()["path"].endswith("-2.csv"))
        outside = os.path.join(self.tmp.name, "export.csv")
        open(outside, "w").close()
        self.assertIsNone(intake_sheet.retire_sheet(outside))
        self.assertTrue(os.path.exists(outside))

    def test_refresh_keeps_counts_and_hand_edited_prices(self):
        path = self.create()["path"]
        rows = intake_sheet.read_sheet(path)
        for r in rows:
            if r["product_id"] == "p1":
                r["quantity"] = "3"
            if r["product_id"] == "p2":
                r["price"] = "1.00"     # edited by hand
        intake_sheet.write_sheet(path, rows)
        newer = [product("s1", "Zombie Apocalypse", "137", [variant("p1", low=80)], market=70),
                 product("s2", "Aim High", "193", [variant("p2", low=30)], market=30),
                 product("s6", "Éomer Test", "50", [variant("p6", low=0)], market=None)]
        with mock.patch.object(intake_sheet.manapool_client, "fetch_single_products", return_value=("t2", newer)), \
             mock.patch.object(intake_sheet, "load_listed", return_value={}), \
             mock.patch.object(intake_sheet.repricer, "load_pins", return_value={}):
            res = intake_sheet.refresh_prices(path, config=CONFIG)
        self.assertEqual(res, {"changed": 1, "kept": 1})
        by_pid = {r["product_id"]: r for r in intake_sheet.read_sheet(path)}
        self.assertEqual((by_pid["p1"]["quantity"], by_pid["p1"]["price"]), ("3", "0.79"))
        self.assertEqual((by_pid["p2"]["price"], by_pid["p2"]["market_low"]), ("1.00", "0.30"))

    def test_stale_after_a_day(self):
        path = self.create()["path"]
        now = datetime.datetime.now(datetime.timezone.utc)
        self.assertFalse(intake_sheet.is_stale(path, now))
        self.assertTrue(intake_sheet.is_stale(path, now + datetime.timedelta(hours=25)))
        os.remove(intake_sheet.meta_path(path))
        self.assertFalse(intake_sheet.is_stale(path, now + datetime.timedelta(days=9)))

    def test_discard_removes_sheet_and_sidecar(self):
        path = self.create()["path"]
        intake_sheet.discard_sheet(path)
        self.assertFalse(os.path.exists(path) or os.path.exists(intake_sheet.meta_path(path)))


class TestHelpers(unittest.TestCase):
    ROWS = [{"name": "Aim High", "number": "193", "price": "0.23", "quantity": "2"},
            {"name": "Éomer Test", "number": "50", "price": "$1.00", "quantity": "1"},
            {"name": "Zombie Apocalypse", "number": "137", "price": "junk", "quantity": "x"}]

    def test_find_row(self):
        self.assertEqual(intake_sheet.find_row(self.ROWS, "137"), 2)
        self.assertEqual(intake_sheet.find_row(self.ROWS, "eomer"), 1)
        self.assertEqual(intake_sheet.find_row(self.ROWS, "high", start=0), 0)   # wraps around
        self.assertIsNone(intake_sheet.find_row(self.ROWS, "nothing"))

    def test_summarize_tolerates_hand_edits(self):
        self.assertEqual(intake_sheet.summarize(self.ROWS), {"lines": 2, "cards": 3, "value_cents": 146})


class TestAdditivePush(unittest.TestCase):
    def test_sends_live_quantity_plus_batch_and_verifies(self):
        before = [{"product_type": "mtg_single", "product_id": "p1", "quantity": 2, "price_cents": 50}]
        after = [{"product_type": "mtg_single", "product_id": "p1", "quantity": 4, "price_cents": 50},
                 {"product_type": "mtg_single", "product_id": "p2", "quantity": 1, "price_cents": 99}]
        with mock.patch.object(manapool_client, "fetch_seller_inventory", side_effect=[before, after]), \
             mock.patch.object(manapool_client, "_post_inventory", return_value=(2, [])) as post:
            res = manapool_client.push_added_stock({"p1": 2, "p2": 1}, {"p1": 50, "p2": 25})
        sent = {e["product_id"]: e["quantity"] for e in post.call_args[0][0]}
        # p1: 2 live (a sold-but-unpicked copy is already off ManaPool) + 2 new = 4; p2 is new.
        self.assertEqual(sent, {"p1": 4, "p2": 1})
        self.assertEqual(res["before"], {"p1": 2, "p2": 0})
        self.assertEqual(res["mismatches"], ["p2"])         # price read back 99, not 25
        self.assertEqual(res["verified_prices"], {"p1": 50})


if __name__ == "__main__":
    unittest.main()
