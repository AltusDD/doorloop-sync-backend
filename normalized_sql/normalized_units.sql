-- 🚧 Normalized View: Units
CREATE OR REPLACE VIEW normalized_units AS
SELECT
    u.id AS unit_id,
    u.unit_number,
    u.bedrooms,
    u.bathrooms,
    u.sqft,
    u.market_rent,
    u.status,
    p.name AS property_name,
    p.addressCity,
    o.full_name AS owner_name
FROM units u
LEFT JOIN properties p ON u.property_id = p.id
LEFT JOIN owners o ON p.owner_id = o.id;
