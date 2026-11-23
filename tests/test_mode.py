"""Pure unit tests for the dev/live mode guard (no database, no network)."""

import os
import shutil
import tempfile
import unittest
from unittest import mock

from station import ROOT, collection_pull, db, manapool_client, mode, pull_list_optimizer


def env(value):
    """Patches MTG_MODE (None = unset)."""
    patched = {k: v for k, v in os.environ.items() if k != "MTG_MODE"}
    if value is not None:
        patched["MTG_MODE"] = value
    return mock.patch.dict(os.environ, patched, clear=True)


class TestModeRules(unittest.TestCase):
    def test_unset_means_live(self):
        with env(None):
            self.assertEqual(mode.current_mode(), mode.LIVE)
        with env(" Dev "):
            self.assertEqual(mode.current_mode(), mode.DEV)

    def test_unknown_mode_fails_closed(self):
        with env("staging"):
            with self.assertRaises(mode.ModeError):
                mode.current_mode()
            with self.assertRaises(mode.ModeError):
                mode.ensure_writes_allowed(mode.LIVE_DB_NAME)

    def test_dev_mode_never_opens_the_live_database(self):
        with env("dev"):
            with self.assertRaises(mode.ModeError):
                mode.ensure_db_allowed(mode.LIVE_DB_NAME)
            mode.ensure_db_allowed(mode.DEV_DB_NAME)
        with env("live"):
            mode.ensure_db_allowed(mode.LIVE_DB_NAME)
            mode.ensure_db_allowed("mtg_inventory_test")  # tests run in live mode on their own database

    def test_writes_need_live_mode_on_the_live_database(self):
        with env("dev"):
            for name in (mode.LIVE_DB_NAME, mode.DEV_DB_NAME):
                with self.assertRaises(mode.ModeError):
                    mode.ensure_writes_allowed(name)
        with env("live"):
            mode.ensure_writes_allowed(mode.LIVE_DB_NAME)
            with self.assertRaises(mode.ModeError):
                mode.ensure_writes_allowed(mode.DEV_DB_NAME)

    def test_front_ends_need_a_matching_pair(self):
        with env("live"):
            self.assertEqual(mode.check_pairing(mode.LIVE_DB_NAME), mode.LIVE)
            with self.assertRaises(mode.ModeError):
                mode.check_pairing(mode.DEV_DB_NAME)
        with env("dev"):
            self.assertEqual(mode.check_pairing(mode.DEV_DB_NAME), mode.DEV)
            with self.assertRaises(mode.ModeError):
                mode.check_pairing(mode.LIVE_DB_NAME)


class TestGuardsInPlace(unittest.TestCase):
    def test_connection_refused_before_connecting(self):
        with env("dev"), mock.patch.object(db, "DB_NAME", mode.LIVE_DB_NAME), \
                mock.patch("psycopg2.connect") as connect:
            with self.assertRaises(mode.ModeError):
                db.get_connection()
        connect.assert_not_called()

    def test_dev_mode_sends_no_manapool_writes(self):
        with env("dev"), mock.patch.object(db, "DB_NAME", mode.DEV_DB_NAME), \
                mock.patch.object(manapool_client, "is_configured", return_value=True), \
                mock.patch("requests.request") as request, mock.patch("requests.post") as post:
            with self.assertRaises(mode.ModeError):
                manapool_client.push_quantities({"pid": 1})
            with self.assertRaises(mode.ModeError):
                manapool_client._request("PUT", "https://example.invalid/fulfillment")
            updated, errors = manapool_client.push_inventory_batch(
                [{"product_id": "pid", "quantity": 1}], aggregate_with_db=False)
            self.assertEqual(updated, 0)
            self.assertIn("Dev mode", errors[0])
            manapool_client._request("GET", "https://example.invalid/prices")  # reads still work
        post.assert_not_called()
        self.assertEqual(request.call_count, 1)
        self.assertEqual(request.call_args[0][0], "GET")


class TestDevFilesStayApart(unittest.TestCase):
    def setUp(self):
        self.out = tempfile.mkdtemp()

    def tearDown(self):
        shutil.rmtree(self.out)

    def test_data_dirs_split_by_mode(self):
        with env("live"):
            self.assertEqual(mode.data_dir("intake"), os.path.join(ROOT, "intake"))
        with env("dev"):
            self.assertEqual(mode.data_dir("intake"), os.path.join(ROOT, "dev", "intake"))
            self.assertEqual(mode.data_dir("audits"), os.path.join(ROOT, "dev", "audits"))

    def test_dev_pull_writes_no_upload_csv(self):
        pick = {"manapool_product_id": "pid", "qty_to_pull": 1}
        with env("dev"), mock.patch.object(manapool_client, "is_configured", return_value=True), \
                mock.patch.object(manapool_client, "push_removed_stock") as push:
            res = collection_pull.sync_manapool([pick], output_dir=self.out)
        push.assert_not_called()
        self.assertEqual((res["mode"], res["csv_path"]), ("none", None))
        self.assertIn("Dev mode", res["errors"][0])
        self.assertEqual(os.listdir(self.out), [])

    def test_dev_pick_sheet_is_marked(self):
        allocated = [{"inventory_id": 1, "box_label": "M2", "set_code": "MID", "section_label": "Sec-1",
                      "index_num": 1, "name": "One", "collector_number": "1", "is_foil": False,
                      "condition": "NM", "qty_to_pull": 1, "order_label": "L-1", "order_id": "id-1"}]
        for value, marked in (("dev", True), ("live", False)):
            with env(value):
                path = pull_list_optimizer.generate_pick_sheet_file(allocated, [], output_dir=self.out)
            with open(path, encoding="utf-8") as f:
                text = f.read()
            self.assertEqual(pull_list_optimizer.DEV_SHEET_WARNING in text, marked)
            self.assertLessEqual(max(len(line) for line in text.splitlines()), pull_list_optimizer.SHEET_WIDTH)


if __name__ == "__main__":
    unittest.main()
