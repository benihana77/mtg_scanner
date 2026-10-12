"""PostgreSQL connection management and database operations module.

Handles environment configuration, connection pooling, schema initialization,
and parameterized queries for cards, inventory, and pricing ledgers.
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
    """Determines the next sequential section label for a given box and set.

    For example, if 'Sec-1' exists for (box_label, set_code), this returns 'Sec-2'.
    If none exist, defaults to 'Sec-1'.

    Args:
        box_label (str): Physical storage box identifier (e.g. 'Box-1').
        set_code (str): 3-4 letter MTG set code (e.g. 'ZNR').

    Returns:
        str: Next available section identifier (e.g. 'Sec-1', 'Sec-2').
    """
    query = """
        SELECT DISTINCT i.section_label
        FROM inventory i
        JOIN cards c ON i.scryfall_id = c.scryfall_id
        WHERE i.box_label = %s AND LOWER(c.set_code) = LOWER(%s);
    """
    with get_db_cursor(commit=False) as cur:
        cur.execute(query, (box_label, set_code))
        rows = cur.fetchall()

    existing_indices: List[int] = []
    pattern = re.compile(r"sec[-_]?(\d+)", re.IGNORECASE)

    for row in rows:
        label = row["section_label"]
        match = pattern.search(label)
        if match:
            try:
                existing_indices.append(int(match.group(1)))
            except ValueError:
                pass

    if not existing_indices:
        return "Sec-1"

    next_num = max(existing_indices) + 1
    return f"Sec-{next_num}"


def get_card_by_set_number(set_code: str, collector_number: str) -> Optional[Dict[str, Any]]:
    """Fetches a card from the local catalog by set code and collector number.

    Args:
        set_code (str): 3-4 letter MTG set code.
        collector_number (str): Card collector number within the set.

    Returns:
        Optional[Dict[str, Any]]: Card record dictionary if found, else None.
    """
    query = """
        SELECT scryfall_id, name, set_code, collector_number, rarity, image_uri, image_hash
        FROM cards
        WHERE LOWER(set_code) = LOWER(%s) AND LOWER(collector_number) = LOWER(%s)
        LIMIT 1;
    """
    with get_db_cursor(commit=False) as cur:
        cur.execute(query, (set_code, collector_number))
        result = cur.fetchone()
        return dict(result) if result else None


def upsert_cards_batch(cards_data: List[Dict[str, Any]]) -> None:
    """Inserts or updates a batch of cards into the cards table.

    Args:
        cards_data (List[Dict[str, Any]]): List of card dictionaries containing:
            - scryfall_id (str)
            - name (str)
            - set_code (str)
            - collector_number (str)
            - rarity (str)
            - image_uri (Optional[str])
            - image_hash (Optional[str])
    """
    if not cards_data:
        return

    query = """
        INSERT INTO cards (scryfall_id, name, set_code, collector_number, rarity, image_uri, image_hash)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (scryfall_id) DO UPDATE SET
            name = EXCLUDED.name,
            set_code = EXCLUDED.set_code,
            collector_number = EXCLUDED.collector_number,
            rarity = COALESCE(EXCLUDED.rarity, cards.rarity),
            image_uri = COALESCE(EXCLUDED.image_uri, cards.image_uri),
            image_hash = COALESCE(EXCLUDED.image_hash, cards.image_hash);
    """
    params = [
        (
            c["scryfall_id"],
            c["name"],
            c["set_code"].upper(),
            c["collector_number"],
            c.get("rarity"),
            c.get("image_uri"),
            c.get("image_hash"),
        )
        for c in cards_data
    ]
    with get_connection() as conn:
        with conn.cursor() as cur:
            extras.execute_batch(cur, query, params)
        conn.commit()


def insert_inventory_batch(inventory_data: List[Dict[str, Any]]) -> None:
    """Inserts a batch of physical inventory records into the inventory table.

    Args:
        inventory_data (List[Dict[str, Any]]): List of inventory dictionaries containing:
            - scryfall_id (str)
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
            manapool_product_id,
            condition,
            is_foil,
            quantity,
            manapool_listed_price,
            box_label,
            section_label,
            index_num
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    params = [
        (
            item["scryfall_id"],
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
    query = """
        SELECT 
            i.box_label,
            c.set_code,
            i.section_label,
            COUNT(i.inventory_id) as total_lines,
            SUM(i.quantity) as total_cards,
            MIN(i.index_num) as min_index,
            MAX(i.index_num) as max_index
        FROM inventory i
        JOIN cards c ON i.scryfall_id = c.scryfall_id
    """
    params: List[Any] = []
    if box_label:
        query += " WHERE i.box_label = %s"
        params.append(box_label)

    query += """
        GROUP BY i.box_label, c.set_code, i.section_label
        ORDER BY i.box_label, c.set_code, i.section_label;
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
    query = """
        SELECT 
            c.set_code,
            COUNT(i.inventory_id) as total_lines,
            SUM(i.quantity) as total_cards,
            ARRAY_AGG(DISTINCT i.section_label) as sections
        FROM inventory i
        JOIN cards c ON i.scryfall_id = c.scryfall_id
        WHERE i.box_label = %s
        GROUP BY c.set_code
        ORDER BY c.set_code;
    """
    with get_db_cursor(commit=False) as cur:
        cur.execute(query, (box_label,))
        return [dict(r) for r in cur.fetchall()]


def relocate_inventory(
    source_box: str,
    target_box: str,
    set_code: str,
    section_label: Optional[str] = None,
    target_section: Optional[str] = None,
) -> int:
    """Moves inventory items for a given set (and optional section) from one box to another.

    Useful when a physical storage box runs out of capacity and sets or sections need
    to be shuffled into another box.

    Args:
        source_box (str): Current box label (e.g. 'Box-1').
        target_box (str): New destination box label (e.g. 'Box-2').
        set_code (str): MTG set code (e.g. 'ZNR').
        section_label (Optional[str]): If provided, only relocates this specific section.
        target_section (Optional[str]): If provided, renames the section in the destination box.
            If None and target_box already has sections for this set, can preserve or renumber.

    Returns:
        int: Number of inventory rows updated.
    """
    source_box = source_box.strip()
    target_box = target_box.strip()
    set_code = set_code.strip().lower()

    if target_section:
        query = """
            UPDATE inventory i
            SET box_label = %s, section_label = %s
            FROM cards c
            WHERE i.scryfall_id = c.scryfall_id
              AND i.box_label = %s
              AND LOWER(c.set_code) = %s
        """
        params = [target_box, target_section.strip(), source_box, set_code]
        if section_label:
            query += " AND i.section_label = %s;"
            params.append(section_label.strip())
        else:
            query += ";"
    else:
        query = """
            UPDATE inventory i
            SET box_label = %s
            FROM cards c
            WHERE i.scryfall_id = c.scryfall_id
              AND i.box_label = %s
              AND LOWER(c.set_code) = %s
        """
        params = [target_box, source_box, set_code]
        if section_label:
            query += " AND i.section_label = %s;"
            params.append(section_label.strip())
        else:
            query += ";"

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, tuple(params))
            updated_count = cur.rowcount
        conn.commit()

    return updated_count


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
