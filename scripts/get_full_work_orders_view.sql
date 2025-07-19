CREATE MATERIALIZED VIEW get_full_work_orders_view AS
SELECT
  w.id AS work_order_id,
  w.title,
  w.status,
  w.category,
  w.priority,
  w.description,
  w.property_id,
  p.name AS property_name,
  u.unit_number,
  v.full_name AS vendor_name,
  w.created_at,
  w.updated_at
FROM doorloop_normalized_work_orders w
LEFT JOIN doorloop_normalized_properties p ON w.property_id = p.id
LEFT JOIN doorloop_normalized_units u ON w.unit_id = u.id
LEFT JOIN doorloop_normalized_vendors v ON w.vendor_id = v.id;
