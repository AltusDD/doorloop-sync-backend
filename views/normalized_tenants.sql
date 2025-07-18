CREATE OR REPLACE VIEW normalized_tenants AS
SELECT
    id AS tenant_id,
    leaseId AS lease_id,
    fullName AS name,
    email,
    phone,
    status,
    created_at
FROM doorloop_raw_tenants
WHERE deleted IS DISTINCT FROM true;