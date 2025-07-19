CREATE OR REPLACE VIEW doorloop_normalized_vendors AS
SELECT
  v.id,
  v.companyName,
  v.phone
FROM doorloop_raw_vendors v;
