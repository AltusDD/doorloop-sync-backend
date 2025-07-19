CREATE OR REPLACE VIEW doorloop_normalized_leases AS
SELECT
  l.id,
  l.leaseStartDate,
  l.leaseEndDate,
  l.unitId,
  l.propertyId,
  l.status
FROM doorloop_raw_leases l;
