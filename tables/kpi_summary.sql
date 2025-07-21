-- Create KPI Summary Table
CREATE TABLE IF NOT EXISTS public.kpi_summary (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type TEXT NOT NULL,
    entity_id UUID NOT NULL,
    metric_name TEXT NOT NULL,
    metric_value NUMERIC NOT NULL,
    metric_unit TEXT,
    recorded_at TIMESTAMPTZ DEFAULT now()
);
