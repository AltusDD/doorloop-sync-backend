CREATE TABLE IF NOT EXISTS doorloop_raw_vendors (
            id TEXT PRIMARY KEY,
            name TEXT,
            serviceType TEXT,
            email TEXT,
            phone TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );