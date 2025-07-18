TRUNCATE TABLE payments RESTART IDENTITY CASCADE;
INSERT INTO payments (doorloopid, lease_id, amount, date, type, status, created_at, updated_at)
SELECT doorloopid, lease_id, amount, date, type, status, created_at, updated_at
FROM normalized_payments;