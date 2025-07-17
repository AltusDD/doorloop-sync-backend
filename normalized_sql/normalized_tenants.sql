-- 🚧 Normalized View: Tenants
CREATE OR REPLACE VIEW normalized_tenants AS
SELECT
    t.id AS tenant_id,
    t.full_name,
    t.email,
    t.phone,
    l.start_date AS lease_start_date,
    l.end_date AS lease_end_date,
    u.unit_number,
    p.name AS property_name
FROM tenants t
LEFT JOIN leases l ON l.tenant_id = t.id
LEFT JOIN units u ON l.unit_id = u.id
LEFT JOIN properties p ON u.property_id = p.id;
