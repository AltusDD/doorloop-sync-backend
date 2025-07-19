CREATE TABLE IF NOT EXISTS doorloop_raw_owners (
            id TEXT PRIMARY KEY,
            name TEXT,
            email TEXT,
            phone TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );