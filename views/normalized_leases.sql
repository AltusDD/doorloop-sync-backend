CREATE OR REPLACE VIEW normalized_leases AS
SELECT
    id AS lease_id,
    unitId AS unit_id,
    propertyId AS property_id,
    startDate,
    endDate,
    status,
    rentAmount,
    depositAmount,
    created_at
FROM doorloop_raw_leases
WHERE deleted IS DISTINCT FROM true;