CREATE OR REPLACE VIEW normalized_units AS
SELECT
    id AS unit_id,
    propertyId AS property_id,
    name AS unit_name,
    bedrooms,
    bathrooms,
    squareFeet AS sqft,
    rentAmount,
    status,
    created_at
FROM doorloop_raw_units
WHERE deleted IS DISTINCT FROM true;