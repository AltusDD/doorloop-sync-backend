CREATE TABLE IF NOT EXISTS normalized_communications (
    id UUID PRIMARY KEY,
    type TEXT,
    direction TEXT,
    subject TEXT,
    body TEXT,
    recipient TEXT,
    related_entity_type TEXT,
    related_entity_id UUID,
    sent_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT now()
);
