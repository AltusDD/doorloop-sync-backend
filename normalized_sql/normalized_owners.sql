-- 🚧 Normalized View: Owners
CREATE OR REPLACE VIEW normalized_owners AS
SELECT
    o.id AS owner_id,
    o.full_name,
    o.phone,
    o.email,
    COUNT(p.id) AS property_count
FROM owners o
LEFT JOIN properties p ON p.owner_id = o.id
GROUP BY o.id;
