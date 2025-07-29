create table if not exists public.legal_case_import_staging (
  id uuid primary key default gen_random_uuid(),
  uploaded_at timestamptz default now(),
  uploaded_by text,
  original_filename text,
  row_number integer not null,
  raw_data jsonb not null,
  status text default 'pending', -- ['pending', 'processed', 'error']
  validation_errors jsonb,
  processed_at timestamptz
);
