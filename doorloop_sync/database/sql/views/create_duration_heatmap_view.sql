
CREATE OR REPLACE VIEW doorloop_pipeline_duration_heatmap AS
SELECT
  entity_type,
  stage,
  DATE_TRUNC('hour', created_at) AS hour_bucket,
  COUNT(*) AS count,
  AVG(duration_ms) AS avg_duration,
  MAX(duration_ms) AS max_duration
FROM doorloop_pipeline_audit
WHERE duration_ms IS NOT NULL
GROUP BY entity_type, stage, hour_bucket
ORDER BY hour_bucket DESC;
