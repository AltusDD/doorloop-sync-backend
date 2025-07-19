CREATE OR REPLACE VIEW doorloop_normalized_owners AS
SELECT
  r.id AS raw_id,
  r.doorloop_id,
  (r.payload_json->>'firstName')::TEXT AS first_name,
  (r.payload_json->>'lastName')::TEXT AS last_name,
  (r.payload_json->>'fullName')::TEXT AS full_name,
  (r.payload_json->>'email')::TEXT AS email,
  (r.payload_json->>'phone')::TEXT AS phone,
  (r.payload_json->>'company')::TEXT AS company,
  (r.payload_json->>'entityType')::TEXT AS entity_type,
  (r.payload_json->>'ein')::TEXT AS ein,
  (r.payload_json->>'ssn')::TEXT AS ssn,
  (r.payload_json->>'address')::TEXT AS address,
  (r.payload_json->>'notes')::TEXT AS notes,
  (r.payload_json->>'createdAt')::TIMESTAMP WITH TIME ZONE AS created_at,
  (r.payload_json->>'updatedAt')::TIMESTAMP WITH TIME ZONE AS updated_at,
  (r.payload_json->'customFields')::JSONB AS custom_fields_json,
  (r.payload_json->'bankAccounts')::JSONB AS bank_accounts_json
FROM doorloop_raw_owners r
WHERE r.doorloop_id IS NOT NULL;
