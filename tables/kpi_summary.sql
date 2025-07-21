-- tables/kpi_summary.sql
CREATE TABLE IF NOT EXISTS public.kpi_summary (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    property_count INTEGER,
    unit_count INTEGER,
    occupied_units INTEGER,
    occupancy_rate NUMERIC,
    delinquent_tenants INTEGER,
    delinquency_rate NUMERIC,
    vacant_units INTEGER,
    average_rent NUMERIC,
    computed_at TIMESTAMP DEFAULT NOW()
);
