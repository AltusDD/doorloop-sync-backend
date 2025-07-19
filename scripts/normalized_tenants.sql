CREATE OR REPLACE VIEW doorloop_normalized_tenants AS
SELECT
  r.id AS raw_id,
  r.doorloop_id,
  (r.payload_json->>'firstName')::TEXT AS first_name,
  (r.payload_json->>'lastName')::TEXT AS last_name,
  (r.payload_json->>'fullName')::TEXT AS full_name,
  (r.payload_json->>'email')::TEXT AS email,
  (r.payload_json->>'phone')::TEXT AS phone,
  (r.payload_json->>'active')::BOOLEAN AS active,
  (r.payload_json->>'birthdate')::DATE AS birthdate,
  (r.payload_json->>'driverLicenseNumber')::TEXT AS driver_license_number,
  (r.payload_json->>'socialSecurityNumber')::TEXT AS ssn,
  (r.payload_json->>'emergencyContact')::TEXT AS emergency_contact,
  (r.payload_json->>'emergencyPhone')::TEXT AS emergency_phone,
  (r.payload_json->>'notes')::TEXT AS notes,
  (r.payload_json->>'createdAt')::TIMESTAMP WITH TIME ZONE AS created_at,
  (r.payload_json->>'updatedAt')::TIMESTAMP WITH TIME ZONE AS updated_at,
  (r.payload_json->'address')::JSONB AS address_json,
  (r.payload_json->'employment')::JSONB AS employment_json,
  (r.payload_json->'customFields')::JSONB AS custom_fields_json,
  (SELECT id FROM leases l WHERE l.doorloop_id = ((r.payload_json->'leases'->>0))) AS lease_id
FROM doorloop_raw_tenants r
WHERE r.doorloop_id IS NOT NULL;
