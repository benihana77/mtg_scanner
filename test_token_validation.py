"""Pure unit tests for token detection and the token validation report (no database)."""

import unittest

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


if __name__ == "__main__":
    unittest.main()
