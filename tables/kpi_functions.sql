-- KPI Computation Function
CREATE OR REPLACE FUNCTION public.compute_and_store_all_kpis()
RETURNS VOID AS $$
BEGIN
    -- Example: Delete existing KPIs (safe only if KPIs are regenerated fully)
    DELETE FROM public.kpi_summary;

    -- Example Insert (to be replaced by real calculations)
    INSERT INTO public.kpi_summary (entity_type, entity_id, metric_name, metric_value)
    SELECT 'property', p.id, 'occupancy_rate', 0.85
    FROM public.doorloop_normalized_properties p;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
