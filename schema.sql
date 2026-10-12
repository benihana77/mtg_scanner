-- Static Scryfall Card Catalog Table
CREATE TABLE IF NOT EXISTS cards (
    scryfall_id VARCHAR(50) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    set_code VARCHAR(10) NOT NULL,
    collector_number VARCHAR(10),
    rarity VARCHAR(20),
    image_uri TEXT,
    image_hash VARCHAR(64)
);

-- Physical Storefront Inventory Table
CREATE TABLE IF NOT EXISTS inventory (
    inventory_id SERIAL PRIMARY KEY,
    scryfall_id VARCHAR(50) REFERENCES cards(scryfall_id),
    manapool_product_id VARCHAR(50),
    condition VARCHAR(10) DEFAULT 'NM',
    is_foil BOOLEAN DEFAULT FALSE,
    quantity INTEGER NOT NULL DEFAULT 1,
    manapool_listed_price DECIMAL(10, 2),
    box_label VARCHAR(50) NOT NULL,
    section_label VARCHAR(10) NOT NULL,
    index_num INTEGER NOT NULL,
    scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily Market Price Ledger Table
CREATE TABLE IF NOT EXISTS price_history (
    price_id SERIAL PRIMARY KEY,
    scryfall_id VARCHAR(50) REFERENCES cards(scryfall_id),
    recorded_date DATE NOT NULL DEFAULT CURRENT_DATE,
    market_price_normal DECIMAL(10, 2),
    market_price_foil DECIMAL(10, 2)
);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_cards_lookup ON cards (set_code, collector_number);
CREATE INDEX IF NOT EXISTS idx_cards_name ON cards (name);
CREATE INDEX IF NOT EXISTS idx_inventory_location ON inventory (box_label, section_label, index_num);
CREATE INDEX IF NOT EXISTS idx_inventory_scryfall ON inventory (scryfall_id);
CREATE INDEX IF NOT EXISTS idx_inventory_product_id ON inventory (manapool_product_id);
