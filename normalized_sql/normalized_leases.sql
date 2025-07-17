-- 🚧 Normalized View: Leases
CREATE OR REPLACE VIEW normalized_leases AS
SELECT
    l.id AS lease_id,
    l.start_date,
    l.end_date,
    l.status,
    t.full_name AS tenant_name,
    u.unit_number,
    p.name AS property_name,
    p.addressCity AS property_city,
    o.full_name AS owner_name
FROM leases l
LEFT JOIN tenants t ON l.tenant_id = t.id
LEFT JOIN units u ON l.unit_id = u.id
LEFT JOIN properties p ON u.property_id = p.id
LEFT JOIN owners o ON p.owner_id = o.id;
