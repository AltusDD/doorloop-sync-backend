CREATE TABLE IF NOT EXISTS property_settings (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    doorloop_id TEXT UNIQUE,
    property_id UUID REFERENCES properties(id),
    setting_key TEXT,
    setting_value TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
