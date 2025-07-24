create table if not exists doorloop_normalized_properties (
    id uuid primary key,
    doorloop_id text unique,
    name text,
    address_street1 text,
    address_city text,
    address_state text,
    zip text,
    property_type text,
    class text,
    status text,
    total_sq_ft numeric,
    unit_count integer,
    occupied_units integer,
    occupancy_rate numeric,
    owner_id uuid,
    created_at timestamp with time zone default now(),
    updated_at timestamp with time zone default now()
);
