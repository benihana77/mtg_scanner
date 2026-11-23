"""Pure unit tests for the development-database tool's refusals (no database, no Docker)."""

import unittest
from unittest import mock

from station import mode
from tools import dev_db


class TestDevDbRefusals(unittest.TestCase):
    def test_never_targets_the_live_database(self):
        with mock.patch.object(dev_db, "database_exists") as exists, mock.patch("subprocess.Popen") as popen:
            with self.assertRaises(ValueError):
                dev_db.refresh(mode.LIVE_DB_NAME, replace=True)
        exists.assert_not_called()
        popen.assert_not_called()

    def test_existing_copy_needs_replace(self):
        with mock.patch.object(dev_db, "database_exists", return_value=True), \
                mock.patch.object(dev_db, "_connect") as connect, mock.patch("subprocess.Popen") as popen:
            with self.assertRaises(ValueError):
                dev_db.refresh(mode.DEV_DB_NAME)
        connect.assert_not_called()
        popen.assert_not_called()


if __name__ == "__main__":
    unittest.main()
