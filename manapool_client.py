"""ManaPool API v1 integration client.

Provides programmatic interaction with the ManaPool storefront API:
- Ingest unfulfilled orders and line items directly into picklist format.
- Push batch inventory updates (quantity, price, custom physical location ID).
- Retrieve active seller catalog and inventory listings.
"""

import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
from dotenv import load_dotenv

import db

load_dotenv()

BASE_URL = os.getenv("MANAPOOL_API_BASE_URL", "https://manapool.com/api/v1")
EMAIL = os.getenv("MANAPOOL_EMAIL", "").strip()
API_TOKEN = os.getenv("MANAPOOL_API_TOKEN", "").strip()


def get_headers() -> Dict[str, str]:
    """Builds standard authentication headers for ManaPool API requests.

    Returns:
        Dict[str, str]: HTTP request headers.
    """
    headers = {
        "User-Agent": "mtg_scanner/1.0",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }
    if EMAIL:
        headers["X-ManaPool-Email"] = EMAIL
    if API_TOKEN:
        headers["X-ManaPool-Access-Token"] = API_TOKEN
    return headers


def is_configured() -> bool:
    """Checks whether valid ManaPool API credentials are present in environment.

    Returns:
        bool: True if both email and access token are configured.
    """
    return bool(EMAIL and API_TOKEN)


def test_connection() -> Tuple[bool, str]:
    """Validates the configured credentials against the ManaPool API.

    Returns:
        Tuple[bool, str]: (Success boolean, descriptive message).
    """
    if not is_configured():
        return False, "MANAPOOL_EMAIL or MANAPOOL_API_TOKEN is not configured in .env"

    url = f"{BASE_URL}/account"
    try:
        response = requests.get(url, headers=get_headers(), timeout=10)
        if response.status_code == 200:
            data = response.json()
            account_name = data.get("name", "Unknown Seller")
            return True, f"Connected to ManaPool account: {account_name}"
        elif response.status_code == 401:
            return False, "Authentication failed (401 Unauthorized). Check email and token."
        else:
            return False, f"ManaPool API responded with status {response.status_code}: {response.text[:200]}"
    except Exception as e:
        return False, f"Connection error: {e}"


def fetch_unfulfilled_orders() -> Tuple[List[Dict[str, Any]], List[str]]:
    """Retrieves all active unfulfilled orders and extracts their individual line items.

    Returns:
        Tuple[List[Dict[str, Any]], List[str]]: A tuple of:
            - List of normalized pick items (Qty, Description, Set, Condition, Finish, Lang, Number, Order ID)
            - List of error or warning strings encountered.
    """
    if not is_configured():
        return [], ["ManaPool API is not configured. Supply MANAPOOL_EMAIL and MANAPOOL_API_TOKEN in .env"]

    orders_url = f"{BASE_URL}/seller/orders"
    params = {"is_unfulfilled": "true", "limit": 100}
    errors: List[str] = []
    pick_items: List[Dict[str, Any]] = []

    try:
        resp = requests.get(orders_url, headers=get_headers(), params=params, timeout=15)
        if resp.status_code != 200:
            return [], [f"Failed to fetch orders (HTTP {resp.status_code}): {resp.text[:200]}"]

        orders_data = resp.json().get("orders", [])
        if not orders_data:
            return [], ["No unfulfilled orders found on ManaPool."]

        for order in orders_data:
            order_id = order.get("id")
            order_label = order.get("label", order_id)
            if not order_id:
                continue

            # Fetch order details with line items
            detail_url = f"{BASE_URL}/seller/orders/{order_id}"
            time.sleep(0.05)
            d_resp = requests.get(detail_url, headers=get_headers(), timeout=15)
            if d_resp.status_code != 200:
                errors.append(f"Failed to fetch details for order {order_label}: HTTP {d_resp.status_code}")
                continue

            order_detail = d_resp.json().get("order", {})
            for item in order_detail.get("items", []):
                product = item.get("product", {})
                single = product.get("single", {})
                
                # Extract fields matching picklist format
                name = single.get("name", "Unknown Card")
                set_code = (single.get("set") or "").upper()
                collector_number = str(single.get("number") or "")
                condition = single.get("condition_id", "NM")
                finish = single.get("finish_id", "NF")
                lang = single.get("language_id", "EN")
                qty = item.get("quantity", 1)
                product_id = item.get("product_id")
                custom_ext_id = item.get("custom_external_id")

                pick_items.append({
                    "Qty": qty,
                    "Description": name,
                    "Set": set_code,
                    "Condition": condition,
                    "Finish": finish,
                    "Lang": lang,
                    "Number": collector_number,
                    "order_id": order_id,
                    "order_label": order_label,
                    "manapool_product_id": product_id,
                    "custom_external_id": custom_ext_id,
                })

        return pick_items, errors

    except Exception as e:
        return [], [f"Exception while retrieving orders: {e}"]


def build_inventory_payload(
    items: List[Dict[str, Any]],
    aggregate_with_db: bool = True,
) -> List[Dict[str, Any]]:
    """Builds the exact JSON entries `push_inventory_batch` would send, without sending them.

    One entry per ManaPool product. With aggregate_with_db=True the quantity is the
    cumulative in-stock total for that product across all boxes and sections.

    Args:
        items (List[Dict[str, Any]]): Same shape as for `push_inventory_batch`.
        aggregate_with_db (bool): Use PostgreSQL totals instead of each item's quantity.

    Returns:
        List[Dict[str, Any]]: Payload entries (product_id, quantity, price_cents, ...).

    Raises:
        Exception: If the PostgreSQL totals cannot be read while aggregating.
    """
    # Filter items with valid product_id
    valid_items = [
        it for it in items
        if (it.get("product_id") or it.get("manapool_product_id"))
    ]
    if not valid_items:
        return []

    # If aggregating with DB, fetch cumulative totals across all boxes
    db_totals: Dict[str, int] = {}
    if aggregate_with_db:
        prod_ids = [str(it.get("product_id") or it.get("manapool_product_id")).strip() for it in valid_items]
        # Fail loudly: falling back to per-item quantities would overwrite storefront totals.
        db_totals = db.get_total_quantities_by_product_ids(prod_ids)

    # Deduplicate by product_id so each product is sent once with its true total
    seen_products = set()
    formatted_payload = []

    for it in valid_items:
        prod_id = str(it.get("product_id") or it.get("manapool_product_id")).strip()
        if prod_id in seen_products:
            continue
        seen_products.add(prod_id)

        # Determine true quantity to send:
        # If DB totals available, use cumulative total; otherwise fall back to item quantity
        if aggregate_with_db and prod_id in db_totals:
            final_qty = db_totals[prod_id]
        else:
            final_qty = int(it.get("quantity", 1))

        entry: Dict[str, Any] = {
            "product_type": "mtg_single",
            "product_id": prod_id,
            "quantity": final_qty,
        }

        price = it.get("price") or it.get("manapool_listed_price")
        if price is not None:
            try:
                entry["price_cents"] = int(round(float(price) * 100))
            except (ValueError, TypeError):
                entry["price_cents"] = None
        else:
            entry["price_cents"] = None

        if it.get("custom_external_id"):
            entry["custom_external_id"] = str(it["custom_external_id"])

        formatted_payload.append(entry)

    return formatted_payload


def push_inventory_batch(
    items: List[Dict[str, Any]],
    aggregate_with_db: bool = True,
) -> Tuple[int, List[str]]:
    """Pushes a list of inventory updates to ManaPool in chunks of up to 2,000 items.

    CRITICAL NOTE ON QUANTITIES:
    The ManaPool API (POST /seller/inventory/product) SETS (replaces) the absolute
    inventory quantity on the storefront. It does NOT increment or add.
    
    When aggregate_with_db=True (default), this method queries PostgreSQL for the
    CUMULATIVE total quantity in stock across all boxes and sections for each product ID.
    This guarantees that adding a new batch of 2 copies to an existing stock of 3 copies
    sends quantity=5 to ManaPool, preventing existing inventory from being overwritten.

    Args:
        items (List[Dict[str, Any]]): List of inventory items containing:
            - product_id or manapool_product_id (str): ManaPool product UUID.
            - quantity (int): Stock quantity (used directly only if aggregate_with_db=False).
            - price or manapool_listed_price (Optional[float]): Price in dollars.
            - custom_external_id (Optional[str]): E.g. 'Box-1:Sec-2:042'.
        aggregate_with_db (bool): If True, replaces item quantity with the total
            in-stock sum from PostgreSQL for that product across all boxes.

    Returns:
        Tuple[int, List[str]]: (Total items updated, list of error messages).
    """
    if not is_configured():
        return 0, ["ManaPool API is not configured. Cannot push inventory."]

    url = f"{BASE_URL}/seller/inventory/product"
    batch_size = 2000
    total_updated = 0
    errors: List[str] = []

    formatted_payload = build_inventory_payload(items, aggregate_with_db)
    if not formatted_payload:
        return 0, ["No valid items with product_id found to update."]

    # Send in chunks of 2,000
    for i in range(0, len(formatted_payload), batch_size):
        chunk = formatted_payload[i : i + batch_size]
        try:
            resp = requests.post(url, headers=get_headers(), json=chunk, timeout=30)
            if resp.status_code in [200, 204]:
                total_updated += len(chunk)
            else:
                errors.append(f"Chunk {i//batch_size + 1} failed: HTTP {resp.status_code} - {resp.text[:300]}")
        except Exception as e:
            errors.append(f"Chunk {i//batch_size + 1} exception: {e}")

    return total_updated, errors


def get_inventory_item(product_id: str, product_type: str = "mtg_single") -> Tuple[Optional[Dict[str, Any]], str]:
    """Reads the live storefront listing for one product (GET, read only).

    Args:
        product_id (str): ManaPool product UUID.
        product_type (str): 'mtg_single' or 'mtg_sealed'.

    Returns:
        Tuple[Optional[Dict[str, Any]], str]: (inventory item or None, status message).
            A 404 means the seller has no listing for that product yet.
    """
    if not is_configured():
        return None, "ManaPool API is not configured."
    url = f"{BASE_URL}/seller/inventory/product/{product_type}/{product_id}"
    resp = requests.get(url, headers=get_headers(), timeout=15)
    if resp.status_code == 200:
        return resp.json().get("inventory"), "ok"
    if resp.status_code == 404:
        return None, "no listing (404)"
    return None, f"HTTP {resp.status_code}: {resp.text[:200]}"


def push_one_product(product_id: str, send: bool = False) -> Dict[str, Any]:
    """Pushes a single product's database total to ManaPool, reading the listing before and after.

    Args:
        product_id (str): ManaPool product UUID present in inventory.
        send (bool): Actually POST. If False, only reads the live listing and shows the payload.

    Returns:
        Dict[str, Any]: payload entry, 'before' / 'after' listings, HTTP result and errors.
    """
    with db.get_db_cursor(commit=False) as cur:
        cur.execute(
            "SELECT manapool_product_id, manapool_listed_price, quantity FROM inventory WHERE manapool_product_id = %s;",
            (product_id,),
        )
        rows = [dict(r) for r in cur.fetchall()]
    if not rows:
        raise ValueError(f"Product {product_id} is not in inventory.")

    payload = build_inventory_payload(rows, aggregate_with_db=True)
    before, before_status = get_inventory_item(product_id)
    result: Dict[str, Any] = {"payload": payload[0], "before": before, "before_status": before_status,
                              "sent": False, "after": None, "errors": []}
    if not send:
        return result

    updated, errors = push_inventory_batch(rows, aggregate_with_db=True)
    after, after_status = get_inventory_item(product_id)
    result.update({"sent": True, "updated": updated, "errors": errors, "after": after, "after_status": after_status})
    return result


def preview_push(
    set_codes: Optional[List[str]] = None,
    baseline_csv: Optional[str] = None,
    output_dir: str = "exports",
) -> Dict[str, Any]:
    """Dry run: shows exactly what a push of the current database would send. Sends nothing.

    Builds the payload for every in-stock or zero-quantity product whose front-face set is
    in `set_codes` (all products if None) and compares it with a baseline ManaPool export,
    which stands in for what is live on the storefront.

    Args:
        set_codes (Optional[List[str]]): Set codes to include (e.g. ['MOM', 'TMOM']).
        baseline_csv (Optional[str]): ManaPool inventory export to compare against.
        output_dir (str): Folder for the preview CSV and the exact JSON payload.

    Returns:
        Dict[str, Any]: rows (per-product comparison), counts, csv_path, json_path.
    """
    import csv
    import datetime
    import json

    query = """
        SELECT i.manapool_product_id, i.manapool_listed_price, i.quantity, i.is_foil,
               sc.name || COALESCE(' // ' || b.name, '') AS name, sc.set_code,
               sc.collector_number || COALESCE('/' || b.collector_number, '') AS collector_number
        FROM inventory i
        JOIN scryfall_cards sc ON i.scryfall_id = sc.scryfall_id
        LEFT JOIN scryfall_cards b ON i.back_scryfall_id = b.scryfall_id
        WHERE i.manapool_product_id IS NOT NULL AND i.manapool_product_id <> ''
    """
    params: List[Any] = []
    if set_codes:
        query += " AND sc.set_code = ANY(%s)"
        params.append([c.upper() for c in set_codes])
    with db.get_db_cursor(commit=False) as cur:
        cur.execute(query + " ORDER BY sc.set_code, sc.collector_number;", tuple(params))
        inv_rows = [dict(r) for r in cur.fetchall()]

    payload = build_inventory_payload(inv_rows, aggregate_with_db=True)

    baseline: Dict[str, Dict[str, str]] = {}
    if baseline_csv:
        with open(baseline_csv, encoding="utf-8-sig") as f:
            for r in csv.DictReader(f):
                baseline[(r.get("product_id") or "").strip()] = r

    info = {r["manapool_product_id"]: r for r in inv_rows}
    rows: List[Dict[str, Any]] = []
    for e in payload:
        pid = e["product_id"]
        r = info[pid]
        live = baseline.get(pid)
        live_qty = int(live.get("quantity") or 0) if live else None
        live_price = float(live["price"]) if live and live.get("price") else None
        new_price = e["price_cents"] / 100 if e["price_cents"] is not None else None
        flags = []
        if baseline_csv and live is None:
            flags.append("NOT IN BASELINE")
        if live_qty is not None and e["quantity"] != live_qty:
            flags.append("QTY CHANGE")
        if new_price is None:
            flags.append("NO PRICE (sends null)")
        elif live_price is not None and abs(new_price - live_price) > 0.001:
            flags.append("PRICE CHANGE")
        rows.append({
            "product_id": pid, "name": r["name"], "set": r["set_code"], "number": r["collector_number"],
            "finish": "FO" if r["is_foil"] else "NF", "live_qty": live_qty, "new_qty": e["quantity"],
            "live_price": live_price, "new_price": new_price, "flags": ", ".join(flags),
        })

    os.makedirs(output_dir, exist_ok=True)
    stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    csv_path = os.path.join(output_dir, f"manapool_push_preview_{stamp}.csv")
    json_path = os.path.join(output_dir, f"manapool_push_preview_{stamp}.json")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()) if rows else ["product_id"])
        writer.writeheader()
        writer.writerows(rows)
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2)

    counts = {
        "products": len(rows),
        "not_in_baseline": sum("NOT IN BASELINE" in r["flags"] for r in rows),
        "qty_change": sum("QTY CHANGE" in r["flags"] for r in rows),
        "price_change": sum("PRICE CHANGE" in r["flags"] for r in rows),
        "no_price": sum("NO PRICE" in r["flags"] for r in rows),
        "unchanged": sum(not r["flags"] for r in rows),
    }
    return {"rows": rows, "counts": counts, "csv_path": os.path.abspath(csv_path), "json_path": os.path.abspath(json_path)}


def main() -> None:
    """CLI entry point: connection test (default) or a push dry run."""
    import argparse

    from tabulate import tabulate

    parser = argparse.ArgumentParser(description="ManaPool API client")
    sub = parser.add_subparsers(dest="command")
    sub.add_parser("test", help="Check credentials with a read-only GET /account (default)")
    p_prev = sub.add_parser("preview-push", help="Dry run: show what a push of the database would send (sends nothing)")
    p_prev.add_argument("--set", dest="sets", action="append", help="Set code to include (repeatable); default all")
    p_prev.add_argument("--baseline", default="backups/inventory-manapool-audited-20260927.csv",
                        help="ManaPool export to compare against (what is live on the storefront)")
    p_prev.add_argument("--output-dir", default="exports")
    p_one = sub.add_parser("push-one", help="Push one product's database total (reads the live listing before/after)")
    p_one.add_argument("--product", required=True, help="ManaPool product UUID")
    p_one.add_argument("--send", action="store_true", help="Actually send; without it only reads and shows the payload")
    args = parser.parse_args()

    if args.command == "push-one":
        def show(label: str, item: Optional[Dict[str, Any]], status: str) -> None:
            if item:
                price = item.get("price_cents")
                print(f"{label}: quantity={item.get('quantity')} price={'-' if price is None else f'${price / 100:.2f}'}")
            else:
                print(f"{label}: {status}")

        res = push_one_product(args.product, send=args.send)
        print(f"Payload: {res['payload']}")
        show("Live before", res["before"], res["before_status"])
        if not res["sent"]:
            print("[DRY RUN - nothing sent] Re-run with --send to push this one product.")
            return
        for e in res["errors"]:
            print(f"[ManaPool error] {e}", file=sys.stderr)
        show("Live after ", res["after"], res["after_status"])
        return

    if args.command == "preview-push":
        res = preview_push(args.sets, args.baseline, args.output_dir)
        changed = [r for r in res["rows"] if r["flags"]]
        if changed:
            print(tabulate(
                [[r["name"][:34], r["set"], r["number"], r["finish"],
                  "-" if r["live_qty"] is None else r["live_qty"], r["new_qty"],
                  "-" if r["live_price"] is None else f"${r['live_price']:.2f}",
                  "-" if r["new_price"] is None else f"${r['new_price']:.2f}", r["flags"]] for r in changed],
                headers=["Card", "Set", "Num", "Fin", "Live Qty", "New Qty", "Live $", "New $", "Flags"],
                tablefmt="simple",
            ))
        c = res["counts"]
        print(f"\n[DRY RUN - nothing sent] {c['products']} products would be pushed: "
              f"{c['unchanged']} unchanged, {c['qty_change']} quantity changes, {c['not_in_baseline']} not in baseline, "
              f"{c['price_change']} price changes, {c['no_price']} without a price.")
        print(f"Comparison CSV: {res['csv_path']}")
        print(f"Exact payload:  {res['json_path']}")
        return

    configured = is_configured()
    print(f"ManaPool API configured: {configured}")
    if configured:
        ok, msg = test_connection()
        print(f"Connection test: {msg}")
    else:
        print("To configure, add MANAPOOL_EMAIL and MANAPOOL_API_TOKEN to .env")


if __name__ == "__main__":
    main()
