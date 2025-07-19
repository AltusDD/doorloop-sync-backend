CREATE OR REPLACE VIEW doorloop_normalized_tenants AS
SELECT
  t.id,
  t.firstName,
  t.lastName,
  t.email
FROM doorloop_raw_tenants t;
