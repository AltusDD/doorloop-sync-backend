CREATE TABLE IF NOT EXISTS public.doorloop_raw_vendors (
    id TEXT PRIMARY KEY,
    data JSONB,
    source_endpoint TEXT,
    inserted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);