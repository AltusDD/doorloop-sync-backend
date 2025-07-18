CREATE OR REPLACE VIEW get_full_properties_view AS
SELECT
    p.property_id,
    p.name AS property_name,
    p.addressStreet1,
    p.addressCity,
    p.addressState,
    p.zip,
    p.propertyType,
    p.class,
    p.status,
    p.totalSqFt,
    p.created_at AS property_created_at,
    o.owner_id,
    o.name AS owner_name,
    o.email AS owner_email,
    o.phone AS owner_phone,
    COUNT(u.unit_id) AS total_units,
    SUM(CASE WHEN u.occupancyStatus = 'occupied' THEN 1 ELSE 0 END) AS occupied_units,
    ROUND(
        100.0 * SUM(CASE WHEN u.occupancyStatus = 'occupied' THEN 1 ELSE 0 END) / NULLIF(COUNT(u.unit_id), 0),
        2
    ) AS occupancy_rate
FROM normalized_properties p
LEFT JOIN normalized_units u ON p.property_id = u.property_id
LEFT JOIN normalized_owners o ON p.owner_id = o.owner_id
GROUP BY
    p.property_id,
    p.name,
    p.addressStreet1,
    p.addressCity,
    p.addressState,
    p.zip,
    p.propertyType,
    p.class,
    p.status,
    p.totalSqFt,
    p.created_at,
    o.owner_id,
    o.name,
    o.email,
    o.phone;