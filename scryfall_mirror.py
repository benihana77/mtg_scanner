"""Local Scryfall mirror: bulk refresh and catalog lookups.

The `scryfall_cards` table is loaded from Scryfall's "Default Cards" bulk file
and is the catalog source of truth. Intake and import resolve cards here and
never call the Scryfall API per card. Refresh is run manually, e.g. before an
intake that contains a newly released set.

Usage:
    python scryfall_mirror.py refresh
    python scryfall_mirror.py status
"""

import argparse
import gzip
import json
import os
import sys
import tempfile
from typing import Any, Dict, Iterable, Iterator, List, Optional, Tuple

import requests
from psycopg2 import extras

import db

BULK_DATA_URL = "https://api.scryfall.com/bulk-data/default-cards"
SCRYFALL_HEADERS = {
    "User-Agent": os.getenv("SCRYFALL_USER_AGENT", "mtg_scanner/1.0"),
    "Accept": "application/json",
}

# A card is a token if and only if its Scryfall layout is one of these.
TOKEN_LAYOUTS = frozenset({"token", "double_faced_token", "emblem"})

UPSERT_BATCH_SIZE = 1000


def is_token_layout(layout: Optional[str]) -> bool:
    """Returns True if a Scryfall layout denotes a token or emblem.

    Args:
        layout (Optional[str]): Scryfall `layout` value.

    Returns:
        bool: True for token, double_faced_token and emblem layouts.
    """
    return (layout or "").strip().lower() in TOKEN_LAYOUTS


def _image_uri(card: Dict[str, Any]) -> Optional[str]:
    """Extracts the normal-size image URL, falling back to the front face."""
    if "image_uris" in card:
        return card["image_uris"].get("normal")
    faces = card.get("card_faces") or []
    if faces:
        return (faces[0].get("image_uris") or {}).get("normal")
    return None


def card_to_row(card: Dict[str, Any]) -> Tuple[Any, ...]:
    """Converts a Scryfall card object into a `scryfall_cards` row tuple.

    Args:
        card (Dict[str, Any]): Scryfall card object.

    Returns:
        Tuple[Any, ...]: Values in `scryfall_cards` column order (minus refreshed_at).
    """
    return (
        card["id"],
        card["name"],
        card["set"].upper(),
        card.get("set_type"),
        str(card["collector_number"]),
        card.get("rarity"),
        card["layout"],
        card.get("type_line"),
        _image_uri(card),
        extras.Json(card),
    )


def download_bulk_file(dest_path: str) -> Dict[str, Any]:
    """Downloads the current Default Cards bulk file (gzipped JSON Lines).

    Args:
        dest_path (str): Local path to write the .jsonl.gz file to.

    Returns:
        Dict[str, Any]: Scryfall bulk-data metadata for the downloaded file.

    Raises:
        requests.HTTPError: If the metadata or file download fails.
    """
    meta_resp = requests.get(BULK_DATA_URL, headers=SCRYFALL_HEADERS, timeout=30)
    meta_resp.raise_for_status()
    meta = meta_resp.json()

    with requests.get(meta["jsonl_download_uri"], headers=SCRYFALL_HEADERS, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        with open(dest_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    return meta


def iter_bulk_cards(path: str) -> Iterator[Dict[str, Any]]:
    """Streams card objects from a gzipped JSON Lines bulk file.

    Args:
        path (str): Path to a .jsonl.gz bulk file.

    Yields:
        Dict[str, Any]: One Scryfall card object per line.
    """
    with gzip.open(path, mode="rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                yield json.loads(line)


def upsert_scryfall_cards(cards: Iterable[Dict[str, Any]]) -> int:
    """Upserts Scryfall card objects into `scryfall_cards` in batches.

    Args:
        cards (Iterable[Dict[str, Any]]): Scryfall card objects.

    Returns:
        int: Number of cards written.
    """
    query = """
        INSERT INTO scryfall_cards (
            scryfall_id, name, set_code, set_type, collector_number,
            rarity, layout, type_line, image_uri, raw
        ) VALUES %s
        ON CONFLICT (scryfall_id) DO UPDATE SET
            name = EXCLUDED.name,
            set_code = EXCLUDED.set_code,
            set_type = EXCLUDED.set_type,
            collector_number = EXCLUDED.collector_number,
            rarity = EXCLUDED.rarity,
            layout = EXCLUDED.layout,
            type_line = EXCLUDED.type_line,
            image_uri = EXCLUDED.image_uri,
            raw = EXCLUDED.raw,
            refreshed_at = CURRENT_TIMESTAMP;
    """
    written = 0
    batch: List[Tuple[Any, ...]] = []
    with db.get_db_cursor() as cur:
        for card in cards:
            batch.append(card_to_row(card))
            if len(batch) >= UPSERT_BATCH_SIZE:
                extras.execute_values(cur, query, batch)
                written += len(batch)
                batch = []
        if batch:
            extras.execute_values(cur, query, batch)
            written += len(batch)
    return written


def refresh_mirror(bulk_path: Optional[str] = None) -> Dict[str, Any]:
    """Downloads (unless given a local file) and loads the Default Cards bulk file.

    Ensures the schema exists first, so this is safe to run on an older database.

    Args:
        bulk_path (Optional[str]): Existing .jsonl.gz file to load instead of downloading.

    Returns:
        Dict[str, Any]: Summary with card count and Scryfall's bulk `updated_at`.
    """
    db.init_db()
    meta: Dict[str, Any] = {}
    tmp_path: Optional[str] = None
    try:
        if bulk_path is None:
            fd, tmp_path = tempfile.mkstemp(suffix=".jsonl.gz")
            os.close(fd)
            print("Downloading Scryfall Default Cards bulk file...")
            meta = download_bulk_file(tmp_path)
            bulk_path = tmp_path
        print(f"Loading '{bulk_path}' into scryfall_cards...")
        count = upsert_scryfall_cards(iter_bulk_cards(bulk_path))
    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.remove(tmp_path)
    return {"cards_written": count, "bulk_updated_at": meta.get("updated_at")}


def get_mirror_status() -> Dict[str, Any]:
    """Returns the mirror's card count and most recent refresh time.

    Returns:
        Dict[str, Any]: {'card_count': int, 'last_refreshed': Optional[datetime]}.
    """
    with db.get_db_cursor(commit=False) as cur:
        cur.execute("SELECT COUNT(*) AS card_count, MAX(refreshed_at) AS last_refreshed FROM scryfall_cards;")
        return dict(cur.fetchone())


def lookup_cards(keys: Iterable[Tuple[str, str]]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    """Resolves (set_code, collector_number) pairs against the mirror in one query.

    Matching is case-insensitive. When the mirror holds more than one printing for a
    pair, the first by scryfall_id wins so results are deterministic.

    Args:
        keys (Iterable[Tuple[str, str]]): (set_code, collector_number) pairs.

    Returns:
        Dict[Tuple[str, str], Dict[str, Any]]: Map from (SET, number lowercased) to the
            card's scryfall_id, name, set_code, set_type, collector_number, rarity,
            layout and type_line. Pairs not found are absent.
    """
    norm = sorted({(s.strip().upper(), n.strip().lower()) for s, n in keys if s and n})
    if not norm:
        return {}
    query = """
        SELECT DISTINCT ON (sc.set_code, LOWER(sc.collector_number))
            sc.scryfall_id, sc.name, sc.set_code, sc.set_type, sc.collector_number,
            sc.rarity, sc.layout, sc.type_line
        FROM scryfall_cards sc
        JOIN UNNEST(%s::text[], %s::text[]) AS k(set_code, num)
          ON sc.set_code = k.set_code AND LOWER(sc.collector_number) = k.num
        ORDER BY sc.set_code, LOWER(sc.collector_number), sc.scryfall_id;
    """
    with db.get_db_cursor(commit=False) as cur:
        cur.execute(query, ([s for s, _ in norm], [n for _, n in norm]))
        return {(r["set_code"], r["collector_number"].lower()): dict(r) for r in cur.fetchall()}


def main() -> None:
    """CLI entry point for the Scryfall mirror."""
    parser = argparse.ArgumentParser(description="Local Scryfall mirror (scryfall_cards)")
    sub = parser.add_subparsers(dest="command", required=True)
    p_refresh = sub.add_parser("refresh", help="Download and load the Default Cards bulk file")
    p_refresh.add_argument("--file", help="Load an already-downloaded .jsonl.gz instead of downloading")
    sub.add_parser("status", help="Show mirror card count and last refresh time")
    args = parser.parse_args()

    if args.command == "refresh":
        result = refresh_mirror(args.file)
        print(f"Loaded {result['cards_written']:,} cards (Scryfall bulk updated {result['bulk_updated_at'] or 'n/a'}).")
    else:
        status = get_mirror_status()
        print(f"scryfall_cards: {status['card_count']:,} cards, last refreshed {status['last_refreshed'] or 'never'}.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
