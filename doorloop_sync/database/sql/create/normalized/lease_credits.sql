CREATE TABLE IF NOT EXISTS lease_credits (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    doorloop_id TEXT UNIQUE,
    lease_id UUID REFERENCES leases(id),
    amount NUMERIC,
    credit_date DATE,
    description TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
