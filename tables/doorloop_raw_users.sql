CREATE TABLE IF NOT EXISTS doorloop_raw_users (
            id TEXT PRIMARY KEY,
            name TEXT,
            email TEXT,
            role TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );