CREATE TABLE IF NOT EXISTS normalized_vendors (
    id UUID PRIMARY KEY,
    name TEXT,
    email TEXT,
    phone TEXT,
    category TEXT,
    notes TEXT,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
