TRUNCATE TABLE units RESTART IDENTITY CASCADE;
INSERT INTO units (doorloopid, name, property_id, bedrooms, bathrooms, squarefeet, rentamount, status, created_at, updated_at)
SELECT doorloopid, name, property_id, bedrooms, bathrooms, squarefeet, rentamount, status, created_at, updated_at
FROM normalized_units;