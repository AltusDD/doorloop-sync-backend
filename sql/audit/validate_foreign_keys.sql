
-- Detect broken foreign key references in normalized tables
SELECT 'leases → units' AS relation, COUNT(*) AS broken_references
FROM doorloop_normalized_leases l
LEFT JOIN doorloop_normalized_units u ON l.unit_id = u.id
WHERE u.id IS NULL

UNION

SELECT 'leases → properties', COUNT(*)
FROM doorloop_normalized_leases l
LEFT JOIN doorloop_normalized_properties p ON l.property_id = p.id
WHERE p.id IS NULL

UNION

SELECT 'leases → tenants', COUNT(*)
FROM doorloop_normalized_leases l
LEFT JOIN doorloop_normalized_tenants t ON l.tenant_id = t.id
WHERE t.id IS NULL

UNION

SELECT 'units → properties', COUNT(*)
FROM doorloop_normalized_units u
LEFT JOIN doorloop_normalized_properties p ON u.property_id = p.id
WHERE p.id IS NULL;
