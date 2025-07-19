CREATE TABLE IF NOT EXISTS public.doorloop_raw_lease_payments (
    id TEXT PRIMARY KEY,
    data JSONB,
    source_endpoint TEXT,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);