"""Dev/live mode: keeps scanning-station development out of the live store.

`MTG_MODE` in `.env` picks the mode:

- `live` (the default when unset: the home computer's TUI, CLI tools and cron jobs) uses the
  live database (`LIVE_DB_NAME`) and may write to ManaPool.
- `dev` (scanning-station development) uses any database except the live one, normally the
  copy made by `python -m tools.dev_db refresh`, and never writes to ManaPool. Reads (catalog,
  prices, live listings) still work, so previews and dry runs do too.

Three checks enforce it: `ensure_db_allowed` on every database connection (dev mode never
opens the live database), `ensure_writes_allowed` before every ManaPool write (live mode on
the live database only) and `check_pairing` when a front end starts (mode and database must
match both ways).

Generated files are kept apart too (`data_dir`): dev mode writes intake sheets, exports and
audit files under `dev/`, so a dev sheet can never be ingested live, dev files never replace
live ones (pick sheets, the needs-count list), and dev mode writes no ManaPool upload CSVs.
"""

import os

from dotenv import load_dotenv

from station import ROOT

load_dotenv()

LIVE = "live"
DEV = "dev"
LIVE_DB_NAME = "mtg_inventory"      # The store's database; every other database is a copy or a test
DEV_DB_NAME = "mtg_inventory_dev"   # Default name of the development copy (tools/dev_db.py)


class ModeError(RuntimeError):
    """The mode, the database and the requested action do not fit together."""


def current_mode() -> str:
    """Returns the mode from MTG_MODE: 'live' (also when unset) or 'dev'.

    Returns:
        str: LIVE or DEV.

    Raises:
        ModeError: If MTG_MODE has any other value (fails closed rather than guessing).
    """
    value = os.getenv("MTG_MODE", "").strip().lower() or LIVE
    if value not in (LIVE, DEV):
        raise ModeError(f"MTG_MODE must be '{LIVE}' or '{DEV}', not {value!r}.")
    return value


def ensure_db_allowed(db_name: str) -> None:
    """Refuses the live database in dev mode. Called before every database connection.

    Args:
        db_name (str): Database about to be opened.

    Raises:
        ModeError: In dev mode on the live database, or if MTG_MODE is invalid.
    """
    if current_mode() == DEV and db_name == LIVE_DB_NAME:
        raise ModeError(f"Dev mode cannot open the live database '{LIVE_DB_NAME}'. Set DB_NAME to a "
                        f"development copy (python -m tools.dev_db refresh creates '{DEV_DB_NAME}').")


def ensure_writes_allowed(db_name: str) -> None:
    """Refuses a ManaPool write unless running in live mode on the live database.

    Args:
        db_name (str): Database the write was computed from.

    Raises:
        ModeError: In dev mode, on any database other than the live one, or if MTG_MODE is invalid.
    """
    if current_mode() == DEV:
        raise ModeError("Dev mode: ManaPool writes are disabled. Nothing was sent.")
    if db_name != LIVE_DB_NAME:
        raise ModeError(f"ManaPool writes need the live database '{LIVE_DB_NAME}', not '{db_name}'. "
                        "Nothing was sent.")


def check_pairing(db_name: str) -> str:
    """Checks that the mode and the database match both ways. Called when a front end starts.

    Args:
        db_name (str): Database the front end will use.

    Returns:
        str: The mode (LIVE or DEV).

    Raises:
        ModeError: Dev mode on the live database, live mode on any other database, or an invalid MTG_MODE.
    """
    mode = current_mode()
    if mode == DEV:
        ensure_db_allowed(db_name)
    elif db_name != LIVE_DB_NAME:
        raise ModeError(f"Live mode needs the live database '{LIVE_DB_NAME}', not '{db_name}'. "
                        "Set MTG_MODE=dev to work on another database.")
    return mode


def data_dir(name: str) -> str:
    """Folder for generated data: `<repo>/<name>` in live mode, `<repo>/dev/<name>` in dev mode.

    Args:
        name (str): 'intake', 'exports' or 'audits'.

    Returns:
        str: Absolute folder path (not created here).
    """
    return os.path.join(ROOT, "dev", name) if current_mode() == DEV else os.path.join(ROOT, name)


# Default export folder for this process (the mode, like the rest of .env, is fixed at start).
EXPORTS_DIR = data_dir("exports")
