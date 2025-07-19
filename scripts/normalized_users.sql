CREATE TABLE IF NOT EXISTS normalized_users (
    id UUID PRIMARY KEY,
    name TEXT,
    email TEXT,
    role TEXT,
    is_active BOOLEAN,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
