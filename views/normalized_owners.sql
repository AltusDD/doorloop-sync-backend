CREATE OR REPLACE VIEW doorloop_normalized_owners AS
SELECT
  o.id,
  o.firstName,
  o.lastName
FROM doorloop_raw_owners o;
