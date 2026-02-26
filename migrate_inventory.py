import requests
import psycopg2
import time

# --- Configuration ---
MANAPOOL_API_URL = "https://manapool.com/api/v1/seller/inventory" # Verify exact base URL
MANAPOOL_API_KEY = "your_manapool_token_here"

DB_HOST = "localhost" 
DB_NAME = "mtg_inventory"
DB_USER = "mtg_admin"
DB_PASS = "supersecretpassword"

def get_scryfall_metadata(scryfall_id):
    """Fetches rarity and collector number using the exact ID provided by ManaPool."""
    time.sleep(0.1) # Respect Scryfall's 10-requests-per-second limit
    
    url = f"https://api.scryfall.com/cards/{scryfall_id}"
    response = requests.get(url)
    
    if response.status_code == 200:
        data = response.json()
        return {
            "collector_number": data.get("collector_number"),
            "rarity": data.get("rarity")
        }
    return {"collector_number": None, "rarity": None}

def migrate_data():
    # 1. Connect to PostgreSQL
    try:
        conn = psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)
        cursor = conn.cursor()
        print("Connected to PostgreSQL successfully.")
    except Exception as e:
        print(f"Database connection failed: {e}")
        return

    headers = {"Authorization": f"Bearer {MANAPOOL_API_KEY}"}
    
    limit = 100
    offset = 0
    has_more = True
    total_migrated = 0

    # 2. Loop through Pagination
    while has_more:
        print(f"Fetching cards from offset {offset}...")
        params = {"limit": limit, "offset": offset}
        response = requests.get(MANAPOOL_API_URL, headers=headers, params=params)
        
        if response.status_code != 200:
            print(f"Failed to fetch data from ManaPool. Status: {response.status_code}")
            break
            
        data = response.json()
        inventory_list = data.get("inventory", [])
        
        if not inventory_list:
            break

        # 3. Process the Batch
        for item in inventory_list:
            # Ensure we are only grabbing singles, not sealed boxes
            if item.get("product_type") != "mtg_single":
                continue
                
            single_data = item["product"]["single"]
            scryfall_id = single_data["scryfall_id"]
            name = single_data["name"]
            set_code = single_data["set"].lower()
            
            # Map ManaPool data types to your Database
            quantity = item.get("quantity", 1)
            condition = single_data.get("condition_id", "NM")
            is_foil = True if single_data.get("finish_id") == "F" else False
            price_decimal = item.get("price_cents", 0) / 100.0

            # Get the missing metadata safely by exact ID
            meta = get_scryfall_metadata(scryfall_id)

            # Insert into Cards Table
            cursor.execute("""
                INSERT INTO cards (scryfall_id, name, set_code, collector_number, rarity)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (scryfall_id) DO NOTHING;
            """, (scryfall_id, name, set_code, meta['collector_number'], meta['rarity']))

            # Insert into Inventory Table
            cursor.execute("""
                INSERT INTO inventory (scryfall_id, condition, is_foil, quantity, manapool_listed_price, box_label)
                VALUES (%s, %s, %s, %s, %s, 'Legacy_ManaPool_Stock');
            """, (scryfall_id, condition, is_foil, quantity, price_decimal))

            total_migrated += quantity

        # 4. Check if there are more pages
        pagination = data.get("pagination", {})
        returned_count = pagination.get("returned", 0)
        
        if returned_count < limit:
            has_more = False # We hit the last page
        else:
            offset += limit # Queue up the next page

        # Commit every 100 cards so you don't lose data if it crashes
        conn.commit() 

    # 5. Clean Up
    cursor.close()
    conn.close()
    print(f"Migration complete! Processed {total_migrated} individual cards.")

if __name__ == "__main__":
    migrate_data()
