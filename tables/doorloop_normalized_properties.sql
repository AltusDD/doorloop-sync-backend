CREATE TABLE IF NOT EXISTS public.doorloop_normalized_properties (
  id UUID PRIMARY KEY,
  doorloop_id TEXT UNIQUE,
  name TEXT,
  property_type TEXT,
  address_street1 TEXT,
  address_city TEXT,
  address_state TEXT,
  address_zip TEXT,
  manager_id UUID,
  class TEXT,
  status TEXT,
  unit_count INTEGER,
  created_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ,
  pictures_json JSONB
);
