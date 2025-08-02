
CREATE OR REPLACE VIEW doorloop_pipeline_mismatch_report AS
SELECT
  raw.entity_type,
  raw.doorloop_id,
  CASE WHEN norm.id IS NOT NULL THEN '✅' ELSE '❌' END AS normalized,
  CASE WHEN audit.id IS NOT NULL THEN '✅' ELSE '❌' END AS audit_logged,
  trace.internal_id,
  trace.stage,
  trace.status,
  trace.trace_timestamp
FROM (
  SELECT DISTINCT entity_type, doorloop_id
  FROM doorloop_pipeline_audit
  WHERE doorloop_id IS NOT NULL
) raw
LEFT JOIN entity_pipeline_trace trace
  ON raw.entity_type = trace.entity_type AND raw.doorloop_id = trace.doorloop_id
LEFT JOIN doorloop_pipeline_audit audit
  ON raw.entity_type = audit.entity_type AND raw.doorloop_id = audit.doorloop_id
     AND audit.status = 'success'
LEFT JOIN (
  SELECT 'property' AS entity_type, doorloop_id, id FROM doorloop_normalized_properties
  UNION ALL
  SELECT 'unit', doorloop_id, id FROM doorloop_normalized_units
  UNION ALL
  SELECT 'lease', doorloop_id, id FROM doorloop_normalized_leases
  UNION ALL
  SELECT 'tenant', doorloop_id, id FROM doorloop_normalized_tenants
  UNION ALL
  SELECT 'owner', doorloop_id, id FROM doorloop_normalized_owners
) norm
  ON raw.entity_type = norm.entity_type AND raw.doorloop_id = norm.doorloop_id;
