CREATE OR REPLACE VIEW normalized_owners AS
SELECT
    id AS owner_id,
    fullName AS name,
    email,
    phone,
    addressStreet1 AS street,
    addressCity AS city,
    addressState AS state,
    zip AS zip_code,
    status,
    created_at
FROM doorloop_raw_owners
WHERE deleted IS DISTINCT FROM true;