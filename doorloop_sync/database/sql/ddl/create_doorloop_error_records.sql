-- Create the Dead Letter Queue (DLQ) table
CREATE TABLE IF NOT EXISTS doorloop_error_records (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type TEXT NOT NULL,
    doorloop_id TEXT NOT NULL,
    raw_data JSONB NOT NULL,
    status TEXT DEFAULT 'unresolved',
    retry_count INTEGER DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Optional: Indexes for performance
CREATE INDEX IF NOT EXISTS idx_doorloop_error_entity_type ON doorloop_error_records(entity_type);
CREATE INDEX IF NOT EXISTS idx_doorloop_error_status ON doorloop_error_records(status);
