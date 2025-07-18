TRUNCATE TABLE owners RESTART IDENTITY CASCADE;
INSERT INTO owners (doorloopid, name, email, phone, created_at, updated_at)
SELECT doorloopid, name, email, phone, created_at, updated_at
FROM normalized_owners;