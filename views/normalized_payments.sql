CREATE OR REPLACE VIEW doorloop_normalized_payments AS
SELECT
  p.id,
  p.leaseId,
  p.amount,
  p.paymentDate
FROM doorloop_raw_lease_payments p;
