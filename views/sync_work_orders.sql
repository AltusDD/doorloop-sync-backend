TRUNCATE TABLE work_orders RESTART IDENTITY CASCADE;
INSERT INTO work_orders (doorloopid, property_id, unit_id, vendor_id, title, status, priority, created_at, updated_at)
SELECT doorloopid, property_id, unit_id, vendor_id, title, status, priority, created_at, updated_at
FROM normalized_work_orders;