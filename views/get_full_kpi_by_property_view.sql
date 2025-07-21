
-- get_full_kpi_by_property_view.sql
CREATE MATERIALIZED VIEW IF NOT EXISTS public.get_full_kpi_by_property_view AS
SELECT
    ks.*,
    p.name AS property_name,
    p.addressCity AS city,
    p.addressState AS state
FROM
    public.kpi_summary ks
JOIN
    public.doorloop_normalized_properties p ON ks.property_id = p.id;
