"""PostgreSQL connection management and database operations module.

Handles environment configuration, connection pooling, schema initialization,
and parameterized queries for inventory and pricing ledgers. Card facts come from
the local Scryfall mirror (`scryfall_cards`, see scryfall_mirror.py).

Token chunks (section labels TOKEN-1, TOKEN-2, ...) mix many real set codes but are
handled as one shelf set named 'TOKENS' for section numbering, summaries and relocation.
"""

import os
import re
import sys
from contextlib import contextmanager
from typing import Any, Dict, Generator, List, Optional, Tuple

import psycopg2
from psycopg2 import extras
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "mtg_inventory")
DB_USER = os.getenv("DB_USER", "mtg_admin")
DB_PASS = os.getenv("DB_PASS", "password")

TOKENS_SET = "TOKENS"
TOKEN_SECTION_PREFIX = "TOKEN-"
# Shelf set of an inventory row `i` joined to scryfall_cards `sc`: token chunks count as 'TOKENS'.
SHELF_SET_SQL = "(CASE WHEN i.section_label ~* '^TOKEN' THEN 'TOKENS' ELSE sc.set_code END)"


def get_connection():
    """Establishes and returns a new psycopg2 connection.

    Returns:
        psycopg2.extensions.connection: Active database connection.

    Raises:
        psycopg2.OperationalError: If unable to connect to the PostgreSQL instance.
    """
    try:
        return psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
        )
    except psycopg2.OperationalError as e:
        print(f"Error connecting to PostgreSQL at {DB_HOST}:{DB_PORT}/{DB_NAME}: {e}", file=sys.stderr)
        raise


@contextmanager
def get_db_cursor(commit: bool = True) -> Generator[psycopg2.extensions.cursor, None, None]:
    """Context manager yielding a PostgreSQL cursor, with automatic commit or rollback.

    Args:
        commit (bool): Whether to commit the transaction upon exiting the context.

    Yields:
        psycopg2.extensions.cursor: Database cursor.
    """
    conn = get_connection()
    try:
        cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
        yield cursor
        if commit:
            conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        cursor.close()
        conn.close()


def init_db(schema_path: str = "schema.sql") -> None:
    """Executes the DDL statements in schema.sql to initialize database tables and indexes.

    Args:
        schema_path (str): Path to the schema SQL file.
    """
    if not os.path.isabs(schema_path):
        base_dir = os.path.dirname(os.path.abspath(__file__))
        schema_path = os.path.join(base_dir, schema_path)

    with open(schema_path, "r", encoding="utf-8") as f:
        schema_sql = f.read()

    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
        conn.commit()
        print("Database schema successfully initialized.")
    finally:
        conn.close()


def get_next_section(box_label: str, set_code: str) -> str:
    """Determines the next sequential section label for a given box and shelf set.

    For a regular set, if 'Sec-1' exists for (box_label, set_code) this returns 'Sec-2';
    if none exist, 'Sec-1'. For the 'TOKENS' shelf set, token chunks are numbered across
    the whole inventory (TOKEN-1, TOKEN-2, ...), regardless of box.

    Args:
        box_label (str): Physical storage box identifier (e.g. 'M3').
        set_code (str): MTG set code (e.g. 'ZNR') or 'TOKENS'.

    Returns:
        str: Next available section identifier (e.g. 'Sec-2' or 'TOKEN-3').
    """
    if set_code.strip().upper() == TOKENS_SET:
        query = "SELECT DISTINCT section_label FROM inventory WHERE section_label ~* '^TOKEN-[0-9]+$';"
        params: Tuple[Any, ...] = ()
        pattern = re.compile(r"token-(\d+)", re.IGNORECASE)
        prefix = TOKEN_SECTION_PREFIX
    else:
        query = f"""
            SELECT DISTINCT i.section_label
            FROM inventory i
            JOIN scryfall_cards sc ON i.scryfall_id = sc.scryfall_id
            WHERE i.box_label = %s AND {SHELF_SET_SQL} = UPPER(%s);
        """
        params = (box_label, set_code)
        pattern = re.compile(r"sec[-_]?(\d+)", re.IGNORECASE)
        prefix = "Sec-"

    with get_db_cursor(commit=False) as cur:
        cur.execute(query, params)
        rows = cur.fetchall()

    existing_indices: List[int] = []
    for row in rows:
        match = pattern.search(row["section_label"])
        if match:
            existing_indices.append(int(match.group(1)))

    next_num = max(existing_indices) + 1 if existing_indices else 1
    return f"{prefix}{next_num}"


def get_token_box() -> Optional[str]:
    """Returns the box that currently holds the token chunks, if any.

    Returns:
        Optional[str]: Box label of the highest-numbered TOKEN-n section, or None.
    """
    query = """
        SELECT box_label
        FROM inventory
        WHERE section_label ~* '^TOKEN-[0-9]+$'
        ORDER BY CAST(SUBSTRING(section_label FROM '[0-9]+$') AS INTEGER) DESC
        LIMIT 1;
    """
    with get_db_cursor(commit=False) as cur:
        cur.execute(query)
        row = cur.fetchone()
        return row["box_label"] if row else None


def insert_inventory_batch(inventory_data: List[Dict[str, Any]]) -> None:
    """Inserts a batch of physical inventory records into the inventory table.

    Args:
        inventory_data (List[Dict[str, Any]]): List of inventory dictionaries containing:
            - scryfall_id (str)
            - back_scryfall_id (Optional[str]): back face of a double-sided token combo
            - manapool_product_id (Optional[str])
            - condition (str)
            - is_foil (bool)
            - quantity (int)
            - manapool_listed_price (Optional[float])
            - box_label (str)
            - section_label (str)
            - index_num (int)
    """
    if not inventory_data:
        return

    query = """
        INSERT INTO inventory (
            scryfall_id,
            back_scryfall_id,
            manapool_product_id,
            condition,
            is_foil,
            quantity,
            manapool_listed_price,
            box_label,
            section_label,
            index_num
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    params = [
        (
            item["scryfall_id"],
            item.get("back_scryfall_id"),
            item.get("manapool_product_id"),
            item.get("condition", "NM"),
            item.get("is_foil", False),
            item.get("quantity", 1),
            item.get("manapool_listed_price"),
            item["box_label"],
            item["section_label"],
            item["index_num"],
        )
        for item in inventory_data
    ]
    with get_connection() as conn:
        with conn.cursor() as cur:
            extras.execute_batch(cur, query, params)
        conn.commit()


def get_inventory_summary(box_label: Optional[str] = None) -> List[Dict[str, Any]]:
    """Fetches an aggregated summary of all physical sections and cards in stock.

    Args:
        box_label (Optional[str]): If supplied, filters the summary to the specified box.

    Returns:
        List[Dict[str, Any]]: List of section summary records.
    """
    query = f"""
        SELECT
            i.box_label,
            {SHELF_SET_SQL} as set_code,
            i.section_label,
            COUNT(i.inventory_id) as total_lines,
            SUM(i.quantity) as total_cards,
            MIN(i.index_num) as min_index,
            MAX(i.index_num) as max_index
        FROM inventory i
        JOIN scryfall_cards sc ON i.scryfall_id = sc.scryfall_id
    """
    params: List[Any] = []
    if box_label:
        query += " WHERE i.box_label = %s"
        params.append(box_label)

    query += """
        GROUP BY i.box_label, 2, i.section_label
        ORDER BY i.box_label, 2, i.section_label;
    """

    with get_db_cursor(commit=False) as cur:
        cur.execute(query, tuple(params))
        return [dict(r) for r in cur.fetchall()]


def get_distinct_boxes() -> List[str]:
    """Returns a sorted list of all distinct box labels currently in inventory.

    Returns:
        List[str]: List of box labels (e.g. ['Box-1', 'Box-2']).
    """
    query = "SELECT DISTINCT box_label FROM inventory ORDER BY box_label;"
    with get_db_cursor(commit=False) as cur:
        cur.execute(query)
        return [r["box_label"] for r in cur.fetchall()]


def get_sets_in_box(box_label: str) -> List[Dict[str, Any]]:
    """Returns all distinct sets and their card counts stored inside a specific box.

    Args:
        box_label (str): Box identifier.

    Returns:
        List[Dict[str, Any]]: List of dicts with 'set_code', 'total_cards', 'sections'.
    """
    query = f"""
        SELECT
            {SHELF_SET_SQL} as set_code,
            COUNT(i.inventory_id) as total_lines,
            SUM(i.quantity) as total_cards,
            ARRAY_AGG(DISTINCT i.section_label) as sections
        FROM inventory i
        JOIN scryfall_cards sc ON i.scryfall_id = sc.scryfall_id
        WHERE i.box_label = %s
        GROUP BY 1
        ORDER BY 1;
    """
    with get_db_cursor(commit=False) as cur:
        cur.execute(query, (box_label,))
        return [dict(r) for r in cur.fetchall()]


def get_shelf_set_sections(set_code: str) -> List[Dict[str, Any]]:
    """Lists every section of a shelf set with its box, row count, copy count and index range.

    Rows with quantity 0 are counted too: they still hold their place in the section.

    Args:
        set_code (str): MTG set code (e.g. 'BLC') or 'TOKENS'.

    Returns:
        List[Dict[str, Any]]: One dict per (box_label, section_label) with 'total_lines',
        'total_cards', 'min_index' and 'max_index', in box then section order. Empty if the
        shelf set is not in inventory.
    """
    query = f"""
        SELECT
            i.box_label,
            i.section_label,
            COUNT(i.inventory_id) as total_lines,
            SUM(i.quantity) as total_cards,
            MIN(i.index_num) as min_index,
            MAX(i.index_num) as max_index
        FROM inventory i
        JOIN scryfall_cards sc ON i.scryfall_id = sc.scryfall_id
        WHERE {SHELF_SET_SQL} = UPPER(%s)
        GROUP BY i.box_label, i.section_label
        ORDER BY i.box_label, LENGTH(i.section_label), i.section_label;
    """
    with get_db_cursor(commit=False) as cur:
        cur.execute(query, (set_code.strip(),))
        return [dict(r) for r in cur.fetchall()]


def find_section_clashes(rows: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    """Finds section labels of one shelf set that are held in more than one box.

    Moving such a set into a single box would merge two sections with overlapping index numbers.

    Args:
        rows (List[Dict[str, Any]]): Rows or sections of one shelf set, each with 'box_label'
            and 'section_label'.

    Returns:
        Dict[str, List[str]]: section_label -> sorted box labels, for each clashing label only.
    """
    boxes: Dict[str, set] = {}
    for r in rows:
        boxes.setdefault(r["section_label"], set()).add(r["box_label"])
    return {sec: sorted(b) for sec, b in boxes.items() if len(b) > 1}


def relocate_inventory(set_code: str, target_box: str) -> int:
    """Moves every section of a shelf set into one box, in a single transaction.

    Used when a physical storage box runs out of capacity: all of the set's sections
    (Sec-1, Sec-2, ...) move to target_box. Pass set_code 'TOKENS' to move every token
    chunk (TOKEN-1, TOKEN-2, ...) instead; a token row is never moved by its real set code.
    Section labels and index numbers are kept, so the shelf order inside each section is
    unchanged. Only box_label is written: box location is local and never sent to ManaPool.

    Args:
        set_code (str): MTG set code (e.g. 'BLC') or 'TOKENS'.
        target_box (str): Destination box label (e.g. 'M20').

    Returns:
        int: Number of inventory rows updated (rows already in target_box are not counted).

    Raises:
        ValueError: If one of the set's section labels is held in more than one box. Nothing
            is changed.
    """
    set_code = set_code.strip()
    target_box = target_box.strip()
    with get_db_cursor() as cur:
        cur.execute(f"""
            SELECT i.box_label, i.section_label
            FROM inventory i
            JOIN scryfall_cards sc ON i.scryfall_id = sc.scryfall_id
            WHERE {SHELF_SET_SQL} = UPPER(%s)
            FOR UPDATE OF i;
        """, (set_code,))
        clashes = find_section_clashes(cur.fetchall())
        if clashes:
            detail = "; ".join(f"{sec} in {', '.join(b)}" for sec, b in sorted(clashes.items()))
            raise ValueError(f"{set_code.upper()} has the same section label in more than one box ({detail}).")

        cur.execute(f"""
            UPDATE inventory i
            SET box_label = %s
            FROM scryfall_cards sc
            WHERE i.scryfall_id = sc.scryfall_id
              AND {SHELF_SET_SQL} = UPPER(%s)
              AND i.box_label <> %s;
        """, (target_box, set_code, target_box))
        return cur.rowcount


def get_total_quantities_by_product_ids(product_ids: List[str]) -> Dict[str, int]:
    """Calculates the cumulative stock quantity for each ManaPool product across ALL boxes and sections.

    Because the ManaPool API sets the absolute total quantity on the storefront rather than
    incrementing, any batch push must send the sum of all copies currently in physical stock.

    Args:
        product_ids (List[str]): List of ManaPool product UUIDs.

    Returns:
        Dict[str, int]: Mapping of product_id -> total in-stock quantity.
    """
    clean_ids = [pid.strip() for pid in product_ids if pid and pid.strip()]
    if not clean_ids:
        return {}

    query = """
        SELECT manapool_product_id, SUM(quantity) as total_qty
        FROM inventory
        WHERE manapool_product_id = ANY(%s)
        GROUP BY manapool_product_id;
    """
    with get_db_cursor(commit=False) as cur:
        cur.execute(query, (list(set(clean_ids)),))
        return {r["manapool_product_id"]: int(r["total_qty"]) for r in cur.fetchall()}


if __name__ == "__main__":
    print(f"Connecting to database '{DB_NAME}' at {DB_HOST}:{DB_PORT} as '{DB_USER}'...")
    init_db()
    summary = get_inventory_summary()
    print(f"Current inventory sections count: {len(summary)}")
