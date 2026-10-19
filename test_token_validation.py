"""Pure unit tests for token detection and the token validation report (no database)."""

import unittest

from unittest import mock

import import_live_inventory as ili
import scryfall_mirror
import token_validation_report as tvr


def _row(set_code: str, num: str, section: str, name: str = "X") -> dict:
    return {
        "inventory_id": 1, "box_label": "M1", "section_label": section, "index_num": 1,
        "is_foil": False, "quantity": 1, "name": name, "set_code": set_code, "collector_number": num,
    }


class TestTokenLayout(unittest.TestCase):
    def test_token_layouts(self):
        for layout in ["token", "double_faced_token", "emblem", "TOKEN"]:
            self.assertTrue(scryfall_mirror.is_token_layout(layout), layout)

    def test_non_token_layouts(self):
        for layout in ["normal", "art_series", "transform", "", None]:
            self.assertFalse(scryfall_mirror.is_token_layout(layout), layout)

    def test_card_to_row_upper_cases_set_and_uses_face_image(self):
        card = {
            "id": "abc", "name": "Demon", "set": "ddr", "set_type": "duel_deck",
            "collector_number": "72", "rarity": "common", "layout": "token",
            "type_line": "Token Creature — Demon",
            "card_faces": [{"image_uris": {"normal": "front.jpg"}}],
        }
        row = scryfall_mirror.card_to_row(card)
        self.assertEqual(row[2], "DDR")
        self.assertEqual(row[6], "token")
        self.assertEqual(row[8], "front.jpg")


class TestTokenSection(unittest.TestCase):
    def test_labels(self):
        self.assertTrue(tvr.is_token_section("TOKENS"))
        self.assertTrue(tvr.is_token_section("TOKEN-12"))
        self.assertFalse(tvr.is_token_section("Sec-1"))
        self.assertFalse(tvr.is_token_section("PROMOS"))


class TestClassifyRows(unittest.TestCase):
    def setUp(self):
        self.mirror = {
            ("DDR", "72"): {"layout": "token"},
            ("DDR", "1"): {"layout": "normal"},
            ("TCMR", "14"): {"layout": "token"},
        }

    def test_token_in_set_section_is_flagged(self):
        flagged = tvr.classify_rows([_row("DDR", "72", "Sec-1")], self.mirror)
        self.assertEqual([r["category"] for r in flagged], ["token_in_set_box"])

    def test_non_token_in_token_section_is_flagged(self):
        flagged = tvr.classify_rows([_row("ddr", "1", "TOKENS")], self.mirror)
        self.assertEqual([r["category"] for r in flagged], ["non_token_in_token_box"])

    def test_correctly_filed_rows_are_not_flagged(self):
        rows = [_row("TCMR", "14", "TOKENS"), _row("DDR", "1", "Sec-2")]
        self.assertEqual(tvr.classify_rows(rows, self.mirror), [])

    def test_unmatched(self):
        flagged = tvr.classify_rows([_row("ZZZ", "9", "Sec-1")], self.mirror)
        self.assertEqual([r["category"] for r in flagged], ["unmatched"])


class TestComboResolution(unittest.TestCase):
    def test_parse_combo_number(self):
        self.assertEqual(scryfall_mirror.parse_combo_number("TMID", "8-11"), (("TMID", "8"), ("TMID", "11")))
        self.assertEqual(scryfall_mirror.parse_combo_number("tmoc", "17-TMOM-10"), (("TMOC", "17"), ("TMOM", "10")))
        self.assertIsNone(scryfall_mirror.parse_combo_number("TMID", "8"))

    def test_resolve_exact_combo_and_same_card(self):
        cards = {
            ("TMID", "8"): {"scryfall_id": "beast", "name": "Beast", "layout": "token"},
            ("TMID", "11"): {"scryfall_id": "spider", "name": "Spider", "layout": "token"},
            ("TMOM", "18"): {"scryfall_id": "inc", "name": "Incubator // Phyrexian", "layout": "double_faced_token"},
            ("TMOM", "12-3"): {"scryfall_id": "hyph", "name": "Hyphen", "layout": "token"},
            ("MOM", "1"): {"scryfall_id": "c1", "name": "Card", "layout": "normal"},
        }

        def fake_lookup(keys):
            return {k: cards[k] for k in {(s.upper(), n.lower()) for s, n in keys} if k in cards}

        with mock.patch.object(scryfall_mirror, "lookup_cards", side_effect=fake_lookup):
            res = scryfall_mirror.resolve_manapool_cards(
                [("TMID", "8-11"), ("TMOM", "18-18"), ("TMOM", "12-3"), ("MOM", "1"), ("TMID", "8-99"), ("MOM", "1-1")]
            )
        self.assertEqual(res[("TMID", "8-11")]["back"]["scryfall_id"], "spider")
        self.assertEqual(res[("TMID", "8-11")]["name"], "Beast // Spider")
        self.assertIsNone(res[("TMOM", "18-18")]["back"])
        self.assertEqual(res[("TMOM", "12-3")]["front"]["scryfall_id"], "hyph")  # exact match wins
        self.assertFalse(res[("MOM", "1")]["is_token"])
        self.assertNotIn(("TMID", "8-99"), res)  # back face missing
        self.assertNotIn(("MOM", "1-1"), res)  # same card but not a double-faced token


class TestResortList(unittest.TestCase):
    def test_unchanged_layout_needs_no_resort(self):
        layout = {("M1", "Sec-1"): [("a", False), ("b", False)], ("M19", "TOKEN-1"): [("t", False)]}
        sets = {("M1", "Sec-1"): ["AAA", "BBB"], ("M19", "TOKEN-1"): ["TMID"]}
        self.assertEqual(ili.build_resort_list(layout, layout, sets), [])

    def test_changes_are_reported_per_set(self):
        # DDR-style: token "b" was filed inside its set's section and moves to the token box.
        old = {("M1", "Sec-1"): [("x", False), ("a", False), ("b", False), ("c", False)]}
        new = {("M1", "Sec-1"): [("x", False), ("a", False), ("c", False)], ("M19", "TOKEN-1"): [("b", False)]}
        sets = {("M1", "Sec-1"): ["AAA", "BBB", "BBB"], ("M19", "TOKEN-1"): ["BBB"]}
        changes = {(c["box_label"], c["set_code"]): c for c in ili.build_resort_list(old, new, sets)}
        self.assertNotIn(("M1", "AAA"), changes)
        self.assertEqual(changes[("M1", "BBB")]["cards_out"], 1)
        self.assertEqual(changes[("M19", "TOKENS")]["cards_in"], 1)


if __name__ == "__main__":
    unittest.main()
