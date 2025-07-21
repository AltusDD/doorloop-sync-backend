#!/bin/bash
echo "🚀 Deploying full normalized schema to Supabase..."

psql "$SUPABASE_DB_URL" <<'EOF'
-- Properties
CREATE TABLE IF NOT EXISTS public.doorloop_normalized_properties (
    id uuid PRIMARY KEY,
    name text,
    address text,
    city text,
    state text,
    zip text,
    active boolean DEFAULT true,
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now()
);

-- Units
CREATE TABLE IF NOT EXISTS public.doorloop_normalized_units (
    id uuid PRIMARY KEY,
    property_id uuid REFERENCES doorloop_normalized_properties(id),
    unit_number text,
    bedroom_count int,
    bathroom_count int,
    square_feet int,
    rent numeric,
    active boolean DEFAULT true,
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now()
);

-- Leases
CREATE TABLE IF NOT EXISTS public.doorloop_normalized_leases (
    id uuid PRIMARY KEY,
    unit_id uuid REFERENCES doorloop_normalized_units(id),
    lease_start date,
    lease_end date,
    rent numeric,
    status text,
    batch text,
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now()
);

-- Tenants
CREATE TABLE IF NOT EXISTS public.doorloop_normalized_tenants (
    id uuid PRIMARY KEY,
    lease_id uuid REFERENCES doorloop_normalized_leases(id),
    full_name text,
    email text,
    phone text,
    acceptedOnTOS boolean DEFAULT false,
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now()
);

-- Owners
CREATE TABLE IF NOT EXISTS public.doorloop_normalized_owners (
    id uuid PRIMARY KEY,
    name text,
    email text,
    phone text,
    active boolean DEFAULT true,
    created_at timestamp DEFAULT now(),
    updated_at timestamp DEFAULT now()
);
EOF

echo "✅ Supabase normalized schema deployed."