CREATE TABLE IF NOT EXISTS doorloop_raw_lease_payments (
            id TEXT PRIMARY KEY,
            leaseId TEXT,
            tenantId TEXT,
            dueDate DATE,
            paidDate DATE,
            amountDue NUMERIC,
            amountPaid NUMERIC,
            status TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );