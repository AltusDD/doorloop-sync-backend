CREATE MATERIALIZED VIEW get_full_tenants_view AS
SELECT
  t.id AS tenant_id,
  t.full_name,
  t.email,
  t.phone,
  t.birthdate,
  t.active,
  l.id AS lease_id,
  l.status AS lease_status,
  u.unit_number,
  p.name AS property_name,
  p.address_city,
  p.address_state,
  t.created_at,
  t.updated_at
FROM doorloop_normalized_tenants t
LEFT JOIN doorloop_normalized_leases l ON t.lease_id = l.id
LEFT JOIN doorloop_normalized_units u ON l.unit_id = u.id
LEFT JOIN doorloop_normalized_properties p ON u.property_id = p.id;
