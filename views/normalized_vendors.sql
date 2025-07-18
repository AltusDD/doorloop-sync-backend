CREATE OR REPLACE VIEW normalized_vendors AS
SELECT
    id AS vendor_id,
    companyName,
    contactName,
    email,
    phone,
    status,
    created_at
FROM doorloop_raw_vendors
WHERE deleted IS DISTINCT FROM true;