CREATE MATERIALIZED VIEW IF NOT EXISTS public.get_full_kpi_by_owner_view AS
SELECT
    o.id AS owner_id,
    o.name AS owner_name,
    COUNT(DISTINCT p.id) AS property_count,
    COUNT(u.id) AS unit_count,
    COUNT(CASE WHEN u.status = 'occupied' THEN 1 END) AS occupied_units,
    ROUND(
        100.0 * COUNT(CASE WHEN u.status = 'occupied' THEN 1 END) / NULLIF(COUNT(u.id), 0),
        2
    ) AS occupancy_rate,
    SUM(l.rent_amount) AS total_rent,
    SUM(CASE WHEN l.status = 'delinquent' THEN l.balance ELSE 0 END) AS total_delinquency
FROM
    doorloop_normalized_owners o
LEFT JOIN doorloop_normalized_properties p ON p.owner_id = o.id
LEFT JOIN doorloop_normalized_units u ON u.property_id = p.id
LEFT JOIN doorloop_normalized_leases l ON l.unit_id = u.id
GROUP BY
    o.id, o.name;