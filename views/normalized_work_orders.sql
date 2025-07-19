CREATE OR REPLACE VIEW doorloop_normalized_work_orders AS
SELECT
  w.id,
  w.status,
  w.description,
  w.propertyId
FROM doorloop_raw_work_orders w;
