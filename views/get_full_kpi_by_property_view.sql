CREATE MATERIALIZED VIEW IF NOT EXISTS public.get_full_kpi_by_property_view AS
SELECT 
    ks.*,
    p.name AS property_name,
    p.addressStreet1,
    p.addressCity,
    p.addressState
FROM kpi_summary ks
JOIN properties p ON ks.property_id = p.id;