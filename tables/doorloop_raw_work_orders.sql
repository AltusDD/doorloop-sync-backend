CREATE TABLE IF NOT EXISTS doorloop_raw_work_orders (
            id TEXT PRIMARY KEY,
            propertyId TEXT,
            unitId TEXT,
            vendorId TEXT,
            description TEXT,
            status TEXT,
            priority TEXT,
            scheduledDate DATE,
            completedDate DATE,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );