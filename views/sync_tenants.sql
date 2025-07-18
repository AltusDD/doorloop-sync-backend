TRUNCATE TABLE tenants RESTART IDENTITY CASCADE;
INSERT INTO tenants (doorloopid, name, email, phone, status, created_at, updated_at)
SELECT doorloopid, name, email, phone, status, created_at, updated_at
FROM normalized_tenants;