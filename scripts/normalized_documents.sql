CREATE TABLE IF NOT EXISTS normalized_documents (
    id UUID PRIMARY KEY,
    name TEXT,
    type TEXT,
    url TEXT,
    related_entity_type TEXT,
    related_entity_id UUID,
    uploaded_by UUID REFERENCES normalized_users(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
