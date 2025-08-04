
-- PATCH_SILENT_TAG: writeback_audit_table
create table if not exists public.writeback_audit_log (
    id uuid primary key default gen_random_uuid(),
    user_id uuid,
    email text,
    entity_type text,
    action text check (action in ('CREATE', 'UPDATE', 'DELETE')),
    payload jsonb,
    status text default 'pending' check (status in ('pending', 'approved', 'rejected')),
    created_at timestamptz default timezone('utc'::text, now()),
    approved_by text,
    approved_at timestamptz
);
