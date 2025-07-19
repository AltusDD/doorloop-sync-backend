CREATE TABLE IF NOT EXISTS normalized_custom_fields (
    id UUID PRIMARY KEY,
    entity_type TEXT,
    entity_id UUID,
    field_name TEXT,
    field_value TEXT,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
