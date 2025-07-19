CREATE TABLE IF NOT EXISTS doorloop_raw_tenants (
            id TEXT PRIMARY KEY,
            firstName TEXT,
            lastName TEXT,
            email TEXT,
            phone TEXT,
            leaseId TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );