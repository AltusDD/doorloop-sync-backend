CREATE TABLE IF NOT EXISTS doorloop_raw_tasks (
            id TEXT PRIMARY KEY,
            workOrderId TEXT,
            assignedTo TEXT,
            dueDate DATE,
            status TEXT,
            description TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );