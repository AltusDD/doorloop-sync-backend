
CREATE OR REPLACE VIEW doorloop_pipeline_batch_lineage AS
SELECT
  batch_id,
  entity_type,
  stage,
  COUNT(*) AS record_count,
  SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) AS successes,
  SUM(CASE WHEN status = 'error' THEN 1 ELSE 0 END) AS failures,
  MIN(created_at) AS started_at,
  MAX(created_at) AS ended_at
FROM doorloop_pipeline_audit
GROUP BY batch_id, entity_type, stage
ORDER BY started_at DESC;
