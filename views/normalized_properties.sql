CREATE OR REPLACE VIEW doorloop_normalized_properties AS
SELECT
  p.id,
  p.name,
  p.address,
  p.status
FROM doorloop_raw_properties p;
