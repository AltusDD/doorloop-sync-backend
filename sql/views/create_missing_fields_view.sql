
CREATE OR REPLACE VIEW doorloop_pipeline_missing_fields AS
SELECT
  entity_type,
  doorloop_id,
  internal_id,
  stage,
  created_at,
  error_details
FROM doorloop_pipeline_audit
WHERE status = 'error'
  AND (
    error_details::text ILIKE '%missing%'
    OR error_details::text ILIKE '%null%'
    OR error_details::text ILIKE '%required%'
  )
ORDER BY created_at DESC;
