CREATE TABLE IF NOT EXISTS doorloop_raw_leases (
            id TEXT PRIMARY KEY,
            unitId TEXT,
            propertyId TEXT,
            startDate DATE,
            endDate DATE,
            term TEXT,
            status TEXT,
            rolloverToAtWill BOOLEAN,
            rentAmount NUMERIC,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );