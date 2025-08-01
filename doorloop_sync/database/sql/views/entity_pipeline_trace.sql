
CREATE OR REPLACE VIEW public.entity_pipeline_trace AS
SELECT
    dpa.doorloop_id,
    dpa.internal_id,
    dpa.entity_type,
    dpa.pipeline_stage,
    dpa.status,
    dpa.error_code,
    dpa.error_message,
    dpa.error_details,
    dpa.event_timestamp,
    dpa.duration_ms,
    dpa.batch_id,
    dpa.record_count,
    dpa.source_checksum,
    dpa.target_checksum,
    dpa.metadata,
    LAG(dpa.event_timestamp) OVER (PARTITION BY dpa.doorloop_id ORDER BY dpa.event_timestamp) AS previous_stage_timestamp,
    LEAD(dpa.event_timestamp) OVER (PARTITION BY dpa.doorloop_id ORDER BY dpa.event_timestamp) AS next_stage_timestamp
FROM
    public.doorloop_pipeline_audit dpa
ORDER BY
    dpa.doorloop_id, dpa.event_timestamp;
