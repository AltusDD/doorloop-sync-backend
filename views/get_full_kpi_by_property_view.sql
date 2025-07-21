CREATE MATERIALIZED VIEW IF NOT EXISTS public.get_full_kpi_by_property_view AS
SELECT
    k.*,
    p.name AS property_name,
    p.addressStreet1,
    p.addressCity,
    p.addressState,
    p.zip
FROM public.kpi_summary k
JOIN public.doorloop_normalized_properties p ON p.id = k.property_id;
