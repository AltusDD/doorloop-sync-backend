CREATE OR REPLACE VIEW doorloop_normalized_payments AS
SELECT
  r.id AS raw_id,
  r.doorloop_id,
  (r.payload_json->>'amount')::NUMERIC AS amount,
  (r.payload_json->>'status')::TEXT AS status,
  (r.payload_json->>'type')::TEXT AS type,
  (r.payload_json->>'method')::TEXT AS payment_method,
  (r.payload_json->>'reference')::TEXT AS reference,
  (r.payload_json->>'date')::DATE AS payment_date,
  (r.payload_json->>'notes')::TEXT AS notes,
  (r.payload_json->>'createdAt')::TIMESTAMP WITH TIME ZONE AS created_at,
  (r.payload_json->>'updatedAt')::TIMESTAMP WITH TIME ZONE AS updated_at,
  (SELECT id FROM leases l WHERE l.doorloop_id = (r.payload_json->>'lease')) AS lease_id
FROM doorloop_raw_payments r
WHERE r.doorloop_id IS NOT NULL;
