-- normalized_tasks.sql - Canonical schema for normalized task data
create table if not exists doorloop_normalized_tasks (
    id uuid primary key default gen_random_uuid(),
    doorloop_id text not null,
    title text,
    description text,
    status text,
    assigned_to_id text,
    property_id text,
    unit_id text,
    due_date date,
    created_at timestamp,
    updated_at timestamp
);
