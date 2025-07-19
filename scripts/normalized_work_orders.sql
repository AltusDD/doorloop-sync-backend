CREATE OR REPLACE VIEW doorloop_normalized_work_orders AS
SELECT
  r.id AS raw_id,
  r.doorloop_id,
  (r.payload_json->>'title')::TEXT AS title,
  (r.payload_json->>'status')::TEXT AS status,
  (r.payload_json->>'category')::TEXT AS category,
  (r.payload_json->>'priority')::TEXT AS priority,
  (r.payload_json->>'description')::TEXT AS description,
  (r.payload_json->>'createdAt')::TIMESTAMP WITH TIME ZONE AS created_at,
  (r.payload_json->>'updatedAt')::TIMESTAMP WITH TIME ZONE AS updated_at,
  (SELECT id FROM vendors v WHERE v.doorloop_id = (r.payload_json->>'vendor')) AS vendor_id,
  (SELECT id FROM properties p WHERE p.doorloop_id = (r.payload_json->>'property')) AS property_id,
  (SELECT id FROM units u WHERE u.doorloop_id = (r.payload_json->>'unit')) AS unit_id,
  (r.payload_json->'pictures')::JSONB AS pictures_json,
  (r.payload_json->'tasks')::JSONB AS tasks_json,
  (r.payload_json->'customFields')::JSONB AS custom_fields_json
FROM doorloop_raw_work_orders r
WHERE r.doorloop_id IS NOT NULL;
