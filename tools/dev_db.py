"""Development database: a full copy of the live database for scanning-station work.

`refresh` copies the live database (`station.mode.LIVE_DB_NAME`) into the development copy
(default `mtg_inventory_dev`) with pg_dump | pg_restore inside the Postgres container, so
boxes, next section numbers and the Scryfall mirror behave exactly like the store's. The live
database is only read. An existing copy is replaced only with `--replace`. Run it on the
computer that hosts the container. `status` shows both databases.

A checkout works on the copy with DB_NAME=mtg_inventory_dev and MTG_MODE=dev in its `.env`;
dev mode never opens the live database and never writes to ManaPool (station/mode.py).

Usage:
    python -m tools.dev_db status
    python -m tools.dev_db refresh [--replace] [--target mtg_inventory_dev]
"""

import argparse
import os
import subprocess
import sys
from typing import Any, Dict, Optional

import psycopg2
from psycopg2 import sql

from station import db, mode

CONTAINER = os.getenv("PG_CONTAINER", "mtg_postgres")


def _connect(dbname: str) -> psycopg2.extensions.connection:
    """Opens `dbname` directly: creating and copying databases is maintenance outside the mode guard."""
    return psycopg2.connect(host=db.DB_HOST, port=db.DB_PORT, dbname=dbname, user=db.DB_USER, password=db.DB_PASS)


def database_exists(name: str) -> bool:
    """Returns True if a database called `name` exists on the server."""
    conn = _connect("postgres")
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s;", (name,))
            return cur.fetchone() is not None
    finally:
        conn.close()


def describe(name: str) -> Optional[Dict[str, Any]]:
    """Size and contents of a database (inventory rows and copies, mirror cards), or None if it does not exist."""
    if not database_exists(name):
        return None
    conn = _connect(name)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT pg_size_pretty(pg_database_size(current_database()));")
            info: Dict[str, Any] = {"size": cur.fetchone()[0]}
            try:
                cur.execute("SELECT count(*), coalesce(sum(quantity), 0) FROM inventory;")
                info["rows"], info["copies"] = cur.fetchone()
                cur.execute("SELECT count(*) FROM scryfall_cards;")
                info["mirror_cards"] = cur.fetchone()[0]
            except psycopg2.Error as e:
                info["error"] = str(e).strip()
    finally:
        conn.close()
    return info


def refresh(target: str = mode.DEV_DB_NAME, replace: bool = False) -> Dict[str, Any]:
    """Replaces `target` with a fresh copy of the live database.

    Args:
        target (str): Development database to (re)create; never the live database.
        replace (bool): Allow dropping an existing `target`.

    Returns:
        Dict[str, Any]: `describe(target)` after the copy.

    Raises:
        ValueError: If `target` is the live database, or exists and `replace` is False.
        RuntimeError: If pg_dump or pg_restore fails (the copy is then incomplete; refresh again).
    """
    if target == mode.LIVE_DB_NAME:
        raise ValueError(f"The target cannot be the live database '{mode.LIVE_DB_NAME}'.")
    if database_exists(target) and not replace:
        raise ValueError(f"'{target}' already exists; re-run with --replace to overwrite it with a fresh copy.")

    admin = _connect("postgres")
    admin.autocommit = True
    try:
        with admin.cursor() as cur:
            cur.execute(sql.SQL("DROP DATABASE IF EXISTS {} WITH (FORCE);").format(sql.Identifier(target)))
            cur.execute(sql.SQL("CREATE DATABASE {};").format(sql.Identifier(target)))
    finally:
        admin.close()

    dump = subprocess.Popen(["docker", "exec", CONTAINER, "pg_dump", "-U", db.DB_USER, "-Fc", mode.LIVE_DB_NAME],
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    restore = subprocess.run(["docker", "exec", "-i", CONTAINER, "pg_restore", "-U", db.DB_USER, "-d", target,
                              "--no-owner"], stdin=dump.stdout, capture_output=True, text=True)
    dump.stdout.close()
    dump_err = dump.stderr.read().decode(errors="replace")
    dump.wait()
    if dump.returncode != 0 or restore.returncode != 0:
        raise RuntimeError(f"Copy failed (pg_dump exit {dump.returncode}, pg_restore exit {restore.returncode}): "
                           f"{(dump_err + restore.stderr).strip()[-1000:]}")
    return describe(target) or {}


def _print_db(label: str, name: str, info: Optional[Dict[str, Any]]) -> None:
    if info is None:
        print(f"{label}: {name} does not exist")
    elif "error" in info:
        print(f"{label}: {name}, {info['size']} ({info['error']})")
    else:
        print(f"{label}: {name}, {info['size']}: {info['rows']:,} inventory rows, {info['copies']:,} copies, "
              f"{info['mirror_cards']:,} mirror cards")


def main() -> None:
    """CLI entry point."""
    parser = argparse.ArgumentParser(description="Development copy of the live database")
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("status", help="Show the live database and the development copy")
    p_refresh = sub.add_parser("refresh", help="Copy the live database into the development database")
    p_refresh.add_argument("--target", default=mode.DEV_DB_NAME, help=f"Database to create (default {mode.DEV_DB_NAME})")
    p_refresh.add_argument("--replace", action="store_true", help="Drop and replace an existing copy")
    p_status = sub.choices["status"]
    p_status.add_argument("--target", default=mode.DEV_DB_NAME, help=f"Development database (default {mode.DEV_DB_NAME})")
    args = parser.parse_args()

    if args.command == "refresh":
        print(f"Copying {mode.LIVE_DB_NAME} into {args.target} (the live database is only read)...")
        try:
            info = refresh(args.target, replace=args.replace)
        except (ValueError, RuntimeError) as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)
        _print_db("Copy", args.target, info)
    else:
        _print_db("Live", mode.LIVE_DB_NAME, describe(mode.LIVE_DB_NAME))
        _print_db("Dev ", args.target, describe(args.target))


if __name__ == "__main__":
    main()
