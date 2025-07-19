CREATE TABLE IF NOT EXISTS normalized_lease_payments (
    id UUID PRIMARY KEY,
    lease_id UUID REFERENCES normalized_leases(id) ON DELETE CASCADE,
    payment_date DATE,
    amount NUMERIC,
    method TEXT,
    status TEXT,
    note TEXT,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
