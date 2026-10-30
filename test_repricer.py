"""Pure unit tests for the repricer, audit and personal-pull rules (no database, no network)."""

import csv
import datetime
import os
import tempfile
import unittest
from unittest import mock

import audit
import collection_pull
import repricer

CONFIG = {
    "pricing": {"adjustment_pct": -1.0, "floor": 0.15},
    "review": {"max_auto_change_pct": 30.0, "max_auto_change_dollars": 0.50},
    "risers": {"window_days": 7, "low_vs_market_ratio": 1.5, "supply_drop_pct": 50.0,
               "market_rise_pct": 50.0, "market_rise_dollars": 0.25},
    "feed": {"max_age_hours": 24},
}


def product(pid="p1", listed=15, foil=False, qty=1, combo=False, sid="s1"):
    return {"product_id": pid, "scryfall_id": sid, "is_combo": combo, "is_foil": foil, "quantity": qty,
            "listed_cents": listed, "name": "Card", "set_code": "SET", "collector_number": "1"}


class TestBasePrice(unittest.TestCase):
    def test_uses_higher_anchor_minus_one_percent(self):
        self.assertEqual(repricer.base_price_cents(15, 23, CONFIG), 23)    # 22.77 -> 23
        self.assertEqual(repricer.base_price_cents(250, 200, CONFIG), 248)  # 247.5 -> 248 (half up)

    def test_single_anchor_and_floor(self):
        self.assertEqual(repricer.base_price_cents(None, 400, CONFIG), 396)
        self.assertEqual(repricer.base_price_cents(10, None, CONFIG), 15)
        self.assertIsNone(repricer.base_price_cents(None, None, CONFIG))


class TestReviewLimit(unittest.TestCase):
    def test_needs_both_percent_and_dollars(self):
        self.assertTrue(repricer.needs_review(15, 87, CONFIG))     # +480% and +$0.72
        self.assertFalse(repricer.needs_review(200, 260, CONFIG))  # +30% exactly, $0.60 -> not > 30%

    def test_big_changes(self):
        self.assertTrue(repricer.needs_review(20, 101, CONFIG))    # +405%, +$0.81
        self.assertTrue(repricer.needs_review(866, 373, CONFIG))   # -57%, -$4.93
        self.assertFalse(repricer.needs_review(38, 87, CONFIG))    # +129% but only $0.49


class TestRisers(unittest.TestCase):
    def test_low_vs_market(self):
        self.assertEqual(repricer.riser_signals(30, 20, None, None, False, CONFIG), ["LOW>MARKET"])
        self.assertEqual(repricer.riser_signals(29, 20, None, None, False, CONFIG), [])

    def test_history_signals(self):
        start = {"available_quantity": 100, "market_price_normal": 0.20, "market_price_foil": 1.00}
        self.assertEqual(repricer.riser_signals(None, 50, 40, start, False, CONFIG), ["SUPPLY DROP", "MARKET RISE"])
        # +$0.25 is needed as well as +50%: 0.20 -> 0.40 is +100% but only +$0.20
        self.assertEqual(repricer.riser_signals(None, 40, 100, start, False, CONFIG), [])
        # foil compares against the foil market history: 1.00 -> 1.40 is only +40%
        self.assertEqual(repricer.riser_signals(None, 140, 100, start, True, CONFIG), [])
        self.assertEqual(repricer.riser_signals(None, 160, 100, start, True, CONFIG), ["MARKET RISE"])


class TestPlan(unittest.TestCase):
    def test_statuses(self):
        products = [
            product("auto", listed=15, sid="a"),
            product("review", listed=20, sid="r"),
            product("same", listed=23, sid="a"),
            product("pinned", listed=50, sid="a"),
            product("fixed", listed=50, sid="a"),
            product("combo", combo=True, sid="a"),
            product("nodata", sid="missing"),
            product("soldout", qty=0, sid="a"),
        ]
        feed = {"a": {"price_cents_nm": 15, "price_market": 23, "available_quantity": 10},
                "r": {"price_cents_nm": 102, "price_market": None, "available_quantity": 1}}
        plan = {i["product_id"]: i for i in repricer.plan_reprice(products, feed, {"pinned": None, "fixed": 75}, {}, CONFIG)}
        self.assertEqual({k: v["status"] for k, v in plan.items()}, {
            "auto": "auto", "review": "review", "same": "unchanged", "pinned": "pinned",
            "fixed": "auto", "combo": "skipped", "nodata": "skipped",
        })
        self.assertEqual(plan["auto"]["target_cents"], 23)
        self.assertEqual(plan["fixed"]["target_cents"], 75)

    def test_foil_uses_foil_fields(self):
        feed = {"a": {"price_cents_nm": 15, "price_market": 23, "price_cents_nm_foil": 100, "price_market_foil": 80}}
        plan = repricer.plan_reprice([product(foil=True, listed=99, sid="a")], feed, {}, {}, CONFIG)
        self.assertEqual(plan[0]["target_cents"], 99)
        self.assertEqual(plan[0]["status"], "unchanged")

    def test_own_lowest_listing_is_not_undercut(self):
        feed = {"held": {"price_cents_nm": 200, "price_market": 100},    # we are the low, above market
                "raised": {"price_cents_nm": 200, "price_market": 250},  # market alone is higher
                "under": {"price_cents_nm": 150, "price_market": 100}}   # someone else is cheaper
        products = [product("held", listed=200, sid="held"), product("raised", listed=200, sid="raised"),
                    product("under", listed=200, sid="under")]
        plan = {i["product_id"]: i for i in repricer.plan_reprice(products, feed, {}, {}, CONFIG)}
        self.assertEqual((plan["held"]["status"], plan["held"]["target_cents"]), ("unchanged", 200))
        self.assertEqual(plan["raised"]["target_cents"], 248)
        self.assertEqual(plan["under"]["target_cents"], 149)

    def test_not_listed_products_are_skipped(self):
        feed = {"a": {"price_cents_nm": 15, "price_market": 23}}
        plan = repricer.plan_reprice([product("gone", sid="a"), product("pinned", sid="a")], feed,
                                     {"pinned": 75}, {}, CONFIG, not_listed={"gone", "pinned"})
        self.assertEqual([(i["status"], i["reason"]) for i in plan], [("skipped", "not listed on ManaPool")] * 2)


def inv_row(iid, pid, set_code, qty, total=None, price=0.15, box="M1", section="Sec-1", idx=1):
    return {"inventory_id": iid, "product_id": pid, "box_label": box, "section_label": section, "index_num": idx,
            "quantity": qty, "is_foil": False, "price": price, "name": f"Card {iid}", "set_code": set_code,
            "collector_number": str(iid), "product_total": qty if total is None else total}


class TestAuditSelection(unittest.TestCase):
    def setUp(self):
        self.rows = (
            [inv_row(i, f"a{i}", "AAA", 5, idx=i) for i in range(1, 21)]          # 20 drifted in AAA
            + [inv_row(100 + i, f"s{i}", "SOI", 3, idx=i) for i in range(1, 6)]   # 5 drifted in SOI (full)
            + [inv_row(200 + i, f"c{i}", "CCC", 2, idx=i) for i in range(1, 51)]  # 50 clean
            + [inv_row(300, "v1", "CCC", 1, price=4.50, idx=99)]                  # valuable, clean
        )
        self.live = {r["product_id"]: (r["quantity"] - 1 if r["set_code"] in ("AAA", "SOI") else r["quantity"]) for r in self.rows}

    def test_parts(self):
        sheet = audit.select_audit(self.rows, self.live, per_set=4, baseline=10, value_threshold=1.0, full_sets=["SOI"])
        parts = {}
        for r in sheet:
            parts.setdefault(r["part"], []).append(r)
        self.assertEqual(len(parts["DRIFT"]), 4)
        self.assertEqual({r["set"] for r in parts["DRIFT"]}, {"AAA"})
        self.assertEqual(len(parts["DRIFT-FULL"]), 5)
        self.assertEqual(len(parts["BASELINE"]), 10)
        self.assertTrue(any(r["product_id"] == "v1" and r["part"] in ("BASELINE", "VALUE") for r in sheet))
        self.assertEqual(len({r["inventory_id"] for r in sheet}), len(sheet))  # no duplicates

    def test_reproducible_and_skips_counted(self):
        a = audit.select_audit(self.rows, self.live, per_set=4, baseline=10)
        b = audit.select_audit(self.rows, self.live, per_set=4, baseline=10)
        self.assertEqual(a, b)
        skip = {r["inventory_id"] for r in a}
        c = audit.select_audit(self.rows, self.live, per_set=4, baseline=10, skip_ids=skip)
        self.assertFalse(skip & {r["inventory_id"] for r in c})

    def test_unlisted_in_stock_products_count_as_drift(self):
        rows = [inv_row(1, "listed", "AAA", 2), inv_row(2, "unlisted", "AAA", 3), inv_row(3, "empty", "AAA", 0)]
        live = {"listed": 2}
        sheet = audit.select_audit(rows, live, per_set=5, baseline=0, value_threshold=99)
        self.assertEqual([(r["product_id"], r["manapool_qty"]) for r in sheet], [("unlisted", "")])
        self.assertEqual(audit.unlisted_products(rows, live), {"unlisted": 3})
        self.assertEqual(audit.plan_push(rows, live), {})

    def test_already_counted_is_bounded_in_time(self):
        old = (datetime.datetime.now() - datetime.timedelta(days=90)).isoformat(timespec="seconds")
        new = datetime.datetime.now().isoformat(timespec="seconds")
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "applied.csv")
            with open(path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["applied_at", "inventory_id"])
                w.writerow([old, 1])
                w.writerow([new, 2])
            with mock.patch.object(audit, "APPLIED_LOG", path):
                self.assertEqual(audit.already_counted(), {2})
                self.assertEqual(audit.already_counted(None), {1, 2})

    def test_plan_push_only_differences(self):
        rows = [inv_row(1, "p1", "AAA", 2, total=5), inv_row(2, "p1", "AAA", 3, total=5), inv_row(3, "p2", "AAA", 4)]
        self.assertEqual(audit.plan_push(rows, {"p1": 1, "p2": 4, "p3": 9}), {"p1": 5})


class TestAuditNumbers(unittest.TestCase):
    def test_excel_style_whole_numbers(self):
        self.assertEqual([audit._whole_number(x) for x in ["3", "3.0", "0", "2.5", "-1", "x"]], [3, 3, 0, None, None, None])


class TestAuditDropAndNeedsCount(unittest.TestCase):
    def test_zero_ids_win_and_only_differences_pushed(self):
        rows = [inv_row(1, "keep", "AAA", 3), inv_row(2, "same", "AAA", 2)]
        live = {"keep": 1, "same": 2, "gone": 5, "gone0": 0}
        self.assertEqual(audit.plan_push(rows, live, {"gone", "gone0", "keep"}), {"keep": 0, "gone": 0})

    def test_needs_count_sheet_skips_counted_and_missing(self):
        rows = [inv_row(1, "a", "CMR", 2), inv_row(2, "b", "NEO", 1), inv_row(3, "c", "NEO", 4)]
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "needs.csv")
            with open(path, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(["inventory_id", "product_id"])
                for iid in (1, 2, 99):
                    w.writerow([iid, "x"])
            with mock.patch.object(audit, "NEEDS_COUNT", path):
                sheet = audit.needs_count_sheet(rows, {"a": 2, "b": 1}, skip_ids={2})
        self.assertEqual([(r["inventory_id"], r["part"]) for r in sheet], [(1, "NEEDS-COUNT")])


def stock_row(iid, pid, price, section="Sec-1", idx=1):
    return {"inventory_id": iid, "scryfall_id": "s1", "back_scryfall_id": None, "name": "Bolt", "set_code": "AAA",
            "collector_number": "1", "is_foil": False, "condition": "NM", "quantity": 2,
            "manapool_listed_price": price, "manapool_product_id": pid, "box_label": "M1",
            "section_label": section, "index_num": idx}


class TestPersonalPull(unittest.TestCase):
    def test_chosen_inventory_row_is_pulled(self):
        stock = [stock_row(1, "cheap", 0.25), stock_row(2, "pricey", 0.50, idx=2)]
        picks, short = collection_pull.plan_pulls(stock, [{"qty": 1, "name": "Bolt", "inventory_id": 2}])
        self.assertEqual([(p["inventory_id"], p["qty_to_pull"]) for p in picks], [(2, 1)])
        self.assertEqual(short, [])

    def test_partial_order_check_counts_as_unchecked(self):
        items = [{"manapool_product_id": "p1", "Qty": 1}]
        with mock.patch.object(collection_pull.manapool_client, "is_configured", return_value=True), \
             mock.patch.object(collection_pull.manapool_client, "fetch_unfulfilled_orders",
                               return_value=(items, ["Failed to fetch details for order 7: HTTP 500"])):
            reserved, status = collection_pull.reserved_by_open_orders()
        self.assertIsNone(reserved)
        self.assertIn("Could not check open orders", status)
        with mock.patch.object(collection_pull.manapool_client, "is_configured", return_value=True), \
             mock.patch.object(collection_pull.manapool_client, "fetch_unfulfilled_orders",
                               return_value=([], ["No unfulfilled orders found on ManaPool."])):
            self.assertEqual(collection_pull.reserved_by_open_orders()[0], {})


if __name__ == "__main__":
    unittest.main()
