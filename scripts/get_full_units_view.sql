CREATE MATERIALIZED VIEW get_full_units_view AS
SELECT
  u.id AS unit_id,
  u.unit_number,
  u.beds,
  u.baths,
  u.sq_ft,
  u.rent_amount,
  u.floor_plan,
  u.unit_condition,
  u.is_rentable,
  u.last_renovated,
  u.property_id,
  p.name AS property_name,
  p.address_city,
  p.address_state,
  l.id AS lease_id,
  l.status AS lease_status,
  l.start_date,
  l.end_date,
  u.created_at,
  u.updated_at
FROM doorloop_normalized_units u
LEFT JOIN doorloop_normalized_properties p ON u.property_id = p.id
LEFT JOIN doorloop_normalized_leases l ON u.id = l.unit_id;
