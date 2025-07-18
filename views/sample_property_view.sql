CREATE OR REPLACE VIEW property_unit_summary AS
SELECT
  p.id AS property_id,
  p.name AS property_name,
  u.id AS unit_id,
  u.unit_name,
  u.bedrooms,
  u.bathrooms,
  u.status
FROM doorloop_raw_properties p
JOIN doorloop_raw_units u ON u.property_id = p.id;