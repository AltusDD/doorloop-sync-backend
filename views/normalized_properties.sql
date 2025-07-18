CREATE OR REPLACE VIEW normalized_properties AS
SELECT
    id AS property_id,
    name AS property_name,
    addressStreet1 AS street,
    addressCity AS city,
    addressState AS state,
    zip AS zip_code,
    propertyType,
    class,
    status,
    created_at
FROM doorloop_raw_properties
WHERE deleted IS DISTINCT FROM true;