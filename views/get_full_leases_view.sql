CREATE OR REPLACE VIEW get_full_leases_view AS
SELECT
    l.lease_id,
    l.unit_id,
    l.property_id,
    l.startDate,
    l.endDate,
    l.rentAmount,
    l.depositAmount,
    l.status,
    l.created_at AS lease_created_at,
    t.tenant_id,
    t.firstName || ' ' || t.lastName AS tenant_name,
    t.email AS tenant_email,
    t.phone AS tenant_phone,
    u.name AS unit_name,
    u.bedrooms,
    u.bathrooms,
    u.sqft,
    p.name AS property_name,
    p.addressStreet1,
    p.addressCity,
    p.addressState,
    p.zip
FROM normalized_leases l
LEFT JOIN normalized_tenants t ON l.tenant_id = t.tenant_id
LEFT JOIN normalized_units u ON l.unit_id = u.unit_id
LEFT JOIN normalized_properties p ON l.property_id = p.property_id;