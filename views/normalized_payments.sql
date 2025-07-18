CREATE OR REPLACE VIEW normalized_payments AS
SELECT
    id AS payment_id,
    leaseId AS lease_id,
    tenantId AS tenant_id,
    amount,
    method,
    paymentDate,
    status,
    created_at
FROM doorloop_raw_lease_payments
WHERE deleted IS DISTINCT FROM true;