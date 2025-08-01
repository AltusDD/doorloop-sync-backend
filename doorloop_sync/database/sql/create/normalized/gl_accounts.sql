CREATE TABLE IF NOT EXISTS gl_accounts (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    doorloop_id TEXT UNIQUE,
    name TEXT,
    account_type TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
