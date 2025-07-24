CREATE MATERIALIZED VIEW get_full_leases_view AS
SELECT
  l.id AS lease_id,
  l.name AS lease_name,
  l.start_date,
  l.end_date,
  l.term,
  l.status,
  l.eviction_pending,
  l.total_balance_due,
  l.total_recurring_rent,
  l.rollover_to_at_will,
  t.id AS tenant_id,
  t.full_name AS tenant_name,
  t.email AS tenant_email,
  u.unit_number,
  u.id AS unit_id,
  p.id AS property_id,
  p.name AS property_name,
  p.address_city,
  p.address_state,
  l.created_at,
  l.updated_at
FROM doorloop_normalized_leases l
LEFT JOIN doorloop_normalized_tenants t ON l.tenant_id = t.id
LEFT JOIN doorloop_normalized_units u ON l.unit_id = u.id
LEFT JOIN doorloop_normalized_properties p ON u.property_id = p.id;
