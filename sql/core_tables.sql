CREATE TABLE IF NOT EXISTS core.products_2 (
    itemid TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    description TEXT,
    fit TEXT,
    fitinfo TEXT,
    fulldescription TEXT,
    url TEXT,
    colorname TEXT[],
    desc2 TEXT[]
);
