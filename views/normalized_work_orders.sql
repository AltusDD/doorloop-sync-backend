CREATE OR REPLACE VIEW normalized_work_orders AS
SELECT
    id AS work_order_id,
    propertyId AS property_id,
    unitId AS unit_id,
    vendorId AS vendor_id,
    title,
    description,
    status,
    dueDate,
    created_at
FROM doorloop_raw_work_orders
WHERE deleted IS DISTINCT FROM true;