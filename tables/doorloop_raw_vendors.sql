create table if not exists public.doorloop_raw_vendors (
  id text primary key,
  data jsonb,
  created_at timestamp with time zone default current_timestamp,
  updated_at timestamp with time zone default current_timestamp
);
