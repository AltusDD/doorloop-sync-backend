CREATE TABLE IF NOT EXISTS normalized_activity_logs (
    id UUID PRIMARY KEY,
    user_id UUID REFERENCES normalized_users(id) ON DELETE SET NULL,
    action TEXT,
    target_entity_type TEXT,
    target_entity_id UUID,
    timestamp TIMESTAMP DEFAULT now(),
    metadata JSONB
);
