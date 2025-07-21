CREATE TABLE IF NOT EXISTS public.kpi_summary (
    id SERIAL PRIMARY KEY,
    property_id INTEGER NOT NULL,
    unit_count INTEGER,
    occupied_units INTEGER,
    occupancy_rate NUMERIC,
    total_rent NUMERIC,
    vacancy_cost NUMERIC,
    delinquency_count INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
