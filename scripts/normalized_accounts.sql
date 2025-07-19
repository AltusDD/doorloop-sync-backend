CREATE TABLE IF NOT EXISTS normalized_accounts (
    id UUID PRIMARY KEY,
    company_name TEXT,
    timezone TEXT,
    country TEXT,
    default_currency TEXT,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
