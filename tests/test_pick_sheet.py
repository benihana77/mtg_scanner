"""Pure unit tests for the printable order pick sheet (no database, no network)."""

import datetime
import shutil
import tempfile
import unittest
from unittest import mock

from station import pull_list_optimizer as plo


def pick(box, set_code, section, index, name, number, qty, order, foil=False):
    return {"inventory_id": index, "box_label": box, "set_code": set_code, "section_label": section,
            "index_num": index, "name": name, "collector_number": number, "is_foil": foil,
            "condition": "NM", "qty_to_pull": qty, "order_label": order, "order_id": f"id-{order}"}


class TestPickSheet(unittest.TestCase):
    def setUp(self):
        self.out = tempfile.mkdtemp()
        self.allocated = [
            pick("M2", "MID", "Sec-1", 45, "Curse of Surveillance", "46", 1, "759534-2667749"),
            pick("M17", "CMM", "Sec-2", 112, "Sakashima of a Thousand Faces, the Long-Named", "89", 2,
                 "764655-2684884", foil=True),
            pick("M19", "ZNR", "TOKEN-2", 8, "Insect // Cat Beast", "5-10", 1, "759534-2667749"),
        ]
        self.unallocated = [{"Description": "Asmoranomardicadaistinaculdacar " * 2, "Set": "MH2", "Number": "186",
                             "is_foil": False, "missing_qty": 1, "order_label": "764655-2684884"}]

    def tearDown(self):
        shutil.rmtree(self.out)

    def sheet(self, orders=True, **kwargs):
        path = plo.generate_pick_sheet_file(
            self.allocated, self.unallocated, output_dir=self.out,
            orders=plo.summarize_orders(self.allocated, self.unallocated) if orders else None, **kwargs)
        with open(path, encoding="utf-8") as f:
            return f.read()

    def test_every_line_fits_a_printed_page(self):
        text = self.sheet(
            notes=["A long warning that has to wrap onto a second line rather than run off the edge of the page "
                   "when the sheet is printed."],
            already_picked=[{"order_label": "750676-2640391", "picked_at": datetime.datetime(2026, 11, 4, 18, 2)}],
        )
        self.assertLessEqual(max(len(line) for line in text.splitlines()), plo.SHEET_WIDTH)
        self.assertNotIn("Seq", text)
        self.assertIn("ALREADY PICKED", text)

    def test_orders_shown_by_key_with_a_legend(self):
        text = self.sheet()
        self.assertIn("A  759534-2667749  2 cards", text)
        self.assertIn("B  764655-2684884  2 cards, 1 MISSING", text)
        rows = [line for line in text.splitlines() if line.endswith("[ ]")]
        self.assertEqual([r.split()[0] for r in rows], ["M2", "M17", "M19"])
        self.assertEqual([r.split()[-3] for r in rows], ["A", "B", "A"])  # last two are "[" "]"
        self.assertIn("FOIL", rows[1])
        self.assertIn("Sakashima of a Thousand", rows[1])  # cut to fit, not wrapped

    def test_picklist_csv_sheet_has_no_order_column(self):
        text = self.sheet(orders=False)
        self.assertNotIn("Ord", text)
        self.assertNotIn("Orders:", text)
        self.assertLessEqual(max(len(line) for line in text.splitlines()), plo.SHEET_WIDTH)


class TestOrderKeys(unittest.TestCase):
    def test_keys_run_a_to_z_then_double(self):
        self.assertEqual([plo.order_key(n) for n in (0, 1, 25, 26, 27, 51, 52)],
                         ["A", "B", "Z", "AA", "AB", "AZ", "BA"])

    def test_commit_sheet_accepts_sheet_keys(self):
        allocated = [pick("M2", "MID", "Sec-1", 1, "One", "1", 1, "L-1"),
                     pick("M2", "MID", "Sec-1", 2, "Two", "2", 1, "L-2")]
        res = {"source": "api", "ledgered": True, "allocated_items": allocated, "unallocated_items": [],
               "orders": plo.summarize_orders(allocated, [])}
        done = {"lines_updated": 1, "orders_recorded": ["L-2"], "orders_skipped": []}
        with mock.patch.object(plo, "commit_pick_deductions", return_value=done) as commit:
            plo.commit_sheet(res, ["b"])
        self.assertEqual([p["order_label"] for p in commit.call_args[0][0]], ["L-2"])


if __name__ == "__main__":
    unittest.main()
