CREATE OR REPLACE VIEW get_full_work_orders_view AS
SELECT
    w.work_order_id,
    w.unit_id,
    w.property_id,
    w.vendor_id,
    w.status,
    w.priority,
    w.category,
    w.description,
    w.created_at AS work_order_created_at,
    v.name AS vendor_name,
    v.email AS vendor_email,
    v.phone AS vendor_phone,
    u.name AS unit_name,
    p.name AS property_name,
    p.addressStreet1,
    p.addressCity,
    p.addressState,
    p.zip
FROM normalized_work_orders w
LEFT JOIN normalized_vendors v ON w.vendor_id = v.vendor_id
LEFT JOIN normalized_units u ON w.unit_id = u.unit_id
LEFT JOIN normalized_properties p ON w.property_id = p.property_id;