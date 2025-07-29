
CREATE OR REPLACE VIEW entity_pipeline_trace AS
SELECT
  a.entity_type,
  a.doorloop_id,
  a.internal_id,
  a.status,
  a.stage,
  a.batch_id,
  a.duration_ms,
  a.error_details,
  COALESCE(a.created_at, now()) AS trace_timestamp
FROM doorloop_pipeline_audit a
WHERE a.status = 'success'
  AND a.doorloop_id IS NOT NULL
  AND a.internal_id IS NOT NULL;
