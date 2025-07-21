CREATE TABLE IF NOT EXISTS public.kpi_summary (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    property_id UUID,
    total_units INT,
    occupied_units INT,
    vacancy_rate NUMERIC,
    average_rent NUMERIC,
    delinquency_rate NUMERIC,
    turnover_rate NUMERIC,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);