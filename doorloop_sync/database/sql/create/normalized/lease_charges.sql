CREATE TABLE IF NOT EXISTS lease_charges (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    doorloop_id TEXT UNIQUE,
    lease_id UUID REFERENCES leases(id),
    amount NUMERIC,
    due_date DATE,
    description TEXT,
    charge_type TEXT,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
