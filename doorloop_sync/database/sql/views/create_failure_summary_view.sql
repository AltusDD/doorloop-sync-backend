
CREATE OR REPLACE VIEW doorloop_pipeline_failure_summary AS
SELECT
  entity_type,
  stage,
  COUNT(*) AS failure_count,
  MAX(created_at) AS last_failure,
  MIN(created_at) AS first_failure,
  STRING_AGG(DISTINCT error_details::text, ', ' ORDER BY error_details::text LIMIT 3) AS example_errors
FROM doorloop_pipeline_audit
WHERE status = 'error'
GROUP BY entity_type, stage
ORDER BY failure_count DESC;
