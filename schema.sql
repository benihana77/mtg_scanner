-- Local Scryfall Mirror (catalog source of truth)
-- Loaded from Scryfall's "Default Cards" bulk file and refreshed manually.
-- Intake and import resolve cards here; they never call the Scryfall API live.
CREATE TABLE IF NOT EXISTS scryfall_cards (
    scryfall_id VARCHAR(50) PRIMARY KEY,
    name TEXT NOT NULL,
    set_code VARCHAR(10) NOT NULL,          -- Upper-cased to match ManaPool exports
    set_type VARCHAR(30),
    collector_number VARCHAR(20) NOT NULL,
    rarity VARCHAR(20),
    layout VARCHAR(30) NOT NULL,            -- token / double_faced_token / emblem => token
    type_line TEXT,
    image_uri TEXT,
    raw JSONB NOT NULL,                     -- Full Scryfall card object
    refreshed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Physical Storefront Inventory Table
CREATE TABLE IF NOT EXISTS inventory (
    inventory_id SERIAL PRIMARY KEY,
    scryfall_id VARCHAR(50) NOT NULL REFERENCES scryfall_cards(scryfall_id),
    manapool_product_id VARCHAR(50),
    condition VARCHAR(10) DEFAULT 'NM',
    is_foil BOOLEAN DEFAULT FALSE,
    quantity INTEGER NOT NULL DEFAULT 1,
    manapool_listed_price DECIMAL(10, 2),
    box_label VARCHAR(50) NOT NULL,
    section_label VARCHAR(10) NOT NULL,     -- Sec-N for sets, TOKEN-N for token chunks
    index_num INTEGER NOT NULL,
    scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily Market Price Ledger Table
CREATE TABLE IF NOT EXISTS price_history (
    price_id SERIAL PRIMARY KEY,
    scryfall_id VARCHAR(50) REFERENCES scryfall_cards(scryfall_id),
    recorded_date DATE NOT NULL DEFAULT CURRENT_DATE,
    market_price_normal DECIMAL(10, 2),
    market_price_foil DECIMAL(10, 2)
);

-- Performance Indexes
CREATE INDEX IF NOT EXISTS idx_scryfall_cards_lookup ON scryfall_cards (set_code, collector_number);
CREATE INDEX IF NOT EXISTS idx_scryfall_cards_name ON scryfall_cards (name);
CREATE INDEX IF NOT EXISTS idx_scryfall_cards_layout ON scryfall_cards (layout);
CREATE INDEX IF NOT EXISTS idx_inventory_location ON inventory (box_label, section_label, index_num);
CREATE INDEX IF NOT EXISTS idx_inventory_scryfall ON inventory (scryfall_id);
CREATE INDEX IF NOT EXISTS idx_inventory_product_id ON inventory (manapool_product_id);
