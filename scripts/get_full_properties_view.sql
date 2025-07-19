CREATE MATERIALIZED VIEW get_full_properties_view AS
SELECT
  p.id AS property_id,
  p.name AS property_name,
  p.type,
  p.class,
  p.status AS property_status,
  p.address_street1,
  p.address_city,
  p.address_state,
  p.address_zip,
  p.bedroom_count,
  p.unit_count,
  p.occupancy_rate,
  o.id AS owner_id,
  o.full_name AS owner_name,
  o.email AS owner_email,
  o.phone AS owner_phone,
  p.created_at,
  p.updated_at
FROM doorloop_normalized_properties p
LEFT JOIN doorloop_normalized_owners o ON (p.owners_json->0->>'id')::TEXT = o.doorloop_id;
