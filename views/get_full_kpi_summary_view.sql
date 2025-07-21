-- KPI Materialized View
CREATE MATERIALIZED VIEW IF NOT EXISTS public.get_full_kpi_summary_view AS
SELECT
    ks.id,
    ks.entity_type,
    ks.entity_id,
    ks.metric_name,
    ks.metric_value,
    ks.metric_unit,
    ks.recorded_at,
    p.name AS property_name
FROM public.kpi_summary ks
LEFT JOIN public.doorloop_normalized_properties p ON ks.entity_id = p.id
WHERE ks.entity_type = 'property';
