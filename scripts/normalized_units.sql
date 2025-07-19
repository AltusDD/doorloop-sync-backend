CREATE OR REPLACE VIEW doorloop_normalized_units AS
SELECT
  r.id AS raw_id,
  r.doorloop_id,
  (r.payload_json->>'name')::TEXT AS unit_number,
  (r.payload_json->>'beds')::NUMERIC AS beds,
  (r.payload_json->>'baths')::NUMERIC AS baths,
  (r.payload_json->>'size')::NUMERIC AS sq_ft,
  (r.payload_json->>'active')::BOOLEAN AS active,
  (r.payload_json->>'marketRent')::NUMERIC AS rent_amount,
  (r.payload_json->>'description')::TEXT AS description,
  (r.payload_json->>'createdAt')::TIMESTAMP WITH TIME ZONE AS created_at,
  (r.payload_json->>'updatedAt')::TIMESTAMP WITH TIME ZONE AS updated_at,
  (SELECT id FROM properties p WHERE p.doorloop_id = (r.payload_json->>'property')) AS property_id,
  (r.payload_json->>'floorPlan')::TEXT AS floor_plan,
  (r.payload_json->>'isRentable')::BOOLEAN AS is_rentable,
  (r.payload_json->>'lastRenovated')::DATE AS last_renovated,
  (r.payload_json->>'condition')::TEXT AS unit_condition,
  (r.payload_json->'address')::JSONB AS address_json,
  (r.payload_json->'rentalApplicationListing')::JSONB AS listing_json,
  (r.payload_json->'amenities')::JSONB AS amenities_json,
  (r.payload_json->'photos')::JSONB AS pictures_json,
  (r.payload_json->'features')::JSONB AS features_json,
  (r.payload_json->'utilities')::JSONB AS utilities_json
FROM doorloop_raw_units r
WHERE r.doorloop_id IS NOT NULL;
