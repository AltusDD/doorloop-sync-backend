
-- kpi_summary.sql
CREATE TABLE IF NOT EXISTS public.kpi_summary (
    id SERIAL PRIMARY KEY,
    property_id INTEGER NOT NULL,
    units_count INTEGER,
    occupied_units INTEGER,
    occupancy_rate NUMERIC,
    total_rent NUMERIC,
    delinquent_rent NUMERIC,
    vacancy_cost NUMERIC,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
