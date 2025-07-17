-- 🚧 Normalized View: Properties
CREATE OR REPLACE VIEW normalized_properties AS
SELECT
    p.id AS property_id,
    p.name,
    p.addressStreet1,
    p.addressCity,
    p.addressState,
    p.zip,
    p.status,
    p.propertyType,
    p.unitCount,
    o.full_name AS owner_name
FROM properties p
LEFT JOIN owners o ON p.owner_id = o.id;
