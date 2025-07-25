-- DLQ Retry Success Metrics Dashboard
CREATE OR REPLACE VIEW doorloop_dlq_dashboard AS
SELECT
  entity_type,
  COUNT(*) AS total,
  COUNT(*) FILTER (WHERE retry_count = 0) AS unprocessed,
  COUNT(*) FILTER (WHERE retry_count BETWEEN 1 AND 4) AS retrying,
  COUNT(*) FILTER (WHERE retry_count >= 5) AS permafail
FROM doorloop_error_records
GROUP BY entity_type
ORDER BY entity_type;
