"""Pure unit tests for the repricer's pricing rules (no database, no network)."""

import unittest

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


if __name__ == "__main__":
    unittest.main()
