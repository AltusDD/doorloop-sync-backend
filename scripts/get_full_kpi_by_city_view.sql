CREATE MATERIALIZED VIEW IF NOT EXISTS public.get_full_kpi_by_city_view AS
SELECT
    p.addressCity AS city,
    COUNT(u.id) AS total_units,
    COUNT(CASE WHEN u.status = 'occupied' THEN 1 END) AS occupied_units,
    ROUND(
        100.0 * COUNT(CASE WHEN u.status = 'occupied' THEN 1 END) / NULLIF(COUNT(u.id), 0),
        2
    ) AS occupancy_rate,
    SUM(l.rent_amount) AS total_rent,
    SUM(CASE WHEN l.status = 'delinquent' THEN l.balance ELSE 0 END) AS total_delinquency
FROM
    doorloop_normalized_properties p
LEFT JOIN doorloop_normalized_units u ON u.property_id = p.id
LEFT JOIN doorloop_normalized_leases l ON l.unit_id = u.id
GROUP BY
    p.addressCity;