import requests
import psycopg2
import time
from io import BytesIO
from PIL import Image
from hashing_engine import generate_phash

# --- Configuration ---
DB_HOST = "localhost"
DB_NAME = "mtg_inventory"
DB_USER = "mtg_admin"
DB_PASS = "supersecretpassword"

def get_db_connection():
    return psycopg2.connect(host=DB_HOST, dbname=DB_NAME, user=DB_USER, password=DB_PASS)

def extract_image_uri(card):
    """Safely extracts the normal image URI, handling double-faced cards."""
    if "image_uris" in card:
        return card["image_uris"].get("normal")
    elif "card_faces" in card and "image_uris" in card["card_faces"][0]:
        return card["card_faces"][0]["image_uris"].get("normal")
    return None

def fetch_and_insert_sets(cursor, set_codes):
    """Fetches specific sets for testing or production and inserts the text data."""
    for code in set_codes:
        print(f"Fetching set: {code}")
        has_more = True
        url = f"https://api.scryfall.com/cards/search?q=set:{code}"
        
        while has_more:
            time.sleep(0.1) # Respect Scryfall's rate limit
            response = requests.get(url).json()
            
            for card in response.get("data", []):
                img_url = extract_image_uri(card)
                if not img_url:
                    continue # Skip cards without images

                cursor.execute("""
                    INSERT INTO cards (scryfall_id, name, set_code, collector_number, rarity, image_uri)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (scryfall_id) DO NOTHING;
                """, (card["id"], card["name"], card["set"], card["collector_number"], card["rarity"], img_url))
            
            has_more = response.get("has_more", False)
            url = response.get("next_page")

# --- Loop 1: The Daily Cron Job ---
def check_for_new_sets():
    """Finds sets on Scryfall that are not in the DB and downloads them."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Get all sets currently in your database
    cursor.execute("SELECT DISTINCT set_code FROM cards;")
    existing_sets = {row[0] for row in cursor.fetchall()}
    
    # Get all sets currently on Scryfall
    response = requests.get("https://api.scryfall.com/sets").json()
    all_sets = response.get("data", [])
    
    new_set_codes = []
    for s in all_sets:
        # Filter for actual playable sets so we don't download tokens or digital-only alchemy cards
        if s.get("set_type") in ["core", "expansion", "masters", "draft_innovation"]:
            if s.get("code") not in existing_sets:
                new_set_codes.append(s.get("code"))
                
    if new_set_codes:
        print(f"Found missing sets to download: {new_set_codes}")
        fetch_and_insert_sets(cursor, new_set_codes)
        conn.commit()
    else:
        print("Database is completely up to date.")
        
    cursor.close()
    conn.close()

# --- Loop 2: The Hashing Engine ---
def process_missing_hashes():
    """Finds rows without hashes, downloads the image in RAM, hashes it, and updates."""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("SELECT scryfall_id, image_uri FROM cards WHERE image_hash IS NULL AND image_uri IS NOT NULL;")
    cards_to_hash = cursor.fetchall()
    
    for scryfall_id, image_uri in cards_to_hash:
        time.sleep(0.1) # Respect Scryfall's rate limit
        try:
            response = requests.get(image_uri)
            if response.status_code == 200:
                # Load image entirely in RAM, never saving to the Pi's SD card
                img = Image.open(BytesIO(response.content))
                card_hash = generate_phash(img)
                
                cursor.execute("""
                    UPDATE cards SET image_hash = %s WHERE scryfall_id = %s;
                """, (card_hash, scryfall_id))
                conn.commit()
                print(f"Successfully hashed: {scryfall_id}")
        except Exception as e:
            print(f"Failed to hash {scryfall_id}: {e}")
            
    cursor.close()
    conn.close()

if __name__ == "__main__":
    # For initial testing, uncomment the following three lines to fetch specific sets:
    # conn = get_db_connection()
    # fetch_and_insert_sets(conn.cursor(), ['4ed', 'm10'])
    # conn.commit()
    
    # Run the hashing loop on whatever is currently in the DB
    process_missing_hashes()
