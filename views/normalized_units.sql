CREATE OR REPLACE VIEW doorloop_normalized_units AS
SELECT
  u.id,
  u.name,
  u.beds,
  u.baths
FROM doorloop_raw_units u;
