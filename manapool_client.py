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

    # Filter items with valid product_id
    valid_items = [
        it for it in items
        if (it.get("product_id") or it.get("manapool_product_id"))
    ]
    if not valid_items:
        return 0, ["No valid items with product_id found to update."]

    # If aggregating with DB, fetch cumulative totals across all boxes
    db_totals: Dict[str, int] = {}
    if aggregate_with_db:
        prod_ids = [str(it.get("product_id") or it.get("manapool_product_id")).strip() for it in valid_items]
        try:
            db_totals = db.get_total_quantities_by_product_ids(prod_ids)
        except Exception as e:
            print(f"[Warning] Failed to query cumulative DB totals for ManaPool push: {e}", file=sys.stderr)

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


if __name__ == "__main__":
    configured = is_configured()
    print(f"ManaPool API configured: {configured}")
    if configured:
        ok, msg = test_connection()
        print(f"Connection test: {msg}")
    else:
        print("To configure, add MANAPOOL_EMAIL and MANAPOOL_API_TOKEN to .env")
