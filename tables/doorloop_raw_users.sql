CREATE TABLE IF NOT EXISTS public.doorloop_raw_users (
    id TEXT PRIMARY KEY,
    data JSONB,
    source_endpoint TEXT,
    inserted_at TIMESTAMPTZ DEFAULT NOW()
);