TRUNCATE TABLE vendors RESTART IDENTITY CASCADE;
INSERT INTO vendors (doorloopid, name, email, phone, company, created_at, updated_at)
SELECT doorloopid, name, email, phone, company, created_at, updated_at
FROM normalized_vendors;