CREATE TABLE IF NOT EXISTS doorloop_raw_units (
            id TEXT PRIMARY KEY,
            propertyId TEXT,
            name TEXT,
            bedrooms INTEGER,
            bathrooms NUMERIC,
            squareFeet NUMERIC,
            marketRent NUMERIC,
            status TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );