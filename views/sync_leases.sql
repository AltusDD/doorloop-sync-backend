TRUNCATE TABLE leases RESTART IDENTITY CASCADE;
INSERT INTO leases (doorloopid, unit_id, tenant_id, start_date, end_date, status, lease_type, rent_amount, created_at, updated_at)
SELECT doorloopid, unit_id, tenant_id, start_date, end_date, status, lease_type, rent_amount, created_at, updated_at
FROM normalized_leases;