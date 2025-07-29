CREATE TABLE IF NOT EXISTS tasks (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    doorloop_id TEXT UNIQUE,
    title TEXT,
    due_date DATE,
    assigned_to TEXT,
    entity_type TEXT,
    entity_id TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
