CREATE TABLE IF NOT EXISTS doorloop_raw_properties (
            id TEXT PRIMARY KEY,
            name TEXT,
            addressStreet1 TEXT,
            addressCity TEXT,
            addressState TEXT,
            zip TEXT,
            propertyType TEXT,
            class TEXT,
            status TEXT,
            totalSqFt NUMERIC,
            unitCount INTEGER,
            ownerId TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );