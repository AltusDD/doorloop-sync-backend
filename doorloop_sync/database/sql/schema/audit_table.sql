
CREATE TABLE public.doorloop_pipeline_audit (
    audit_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    entity_type TEXT NOT NULL,
    doorloop_id TEXT,
    internal_id UUID,
    pipeline_stage TEXT NOT NULL,
    status TEXT NOT NULL,
    record_count INT,
    error_code TEXT,
    error_message TEXT,
    error_details JSONB,
    event_timestamp TIMESTAMPTZ NOT NULL DEFAULT now(),
    duration_ms INT,
    batch_id UUID,
    source_checksum TEXT,
    target_checksum TEXT,
    metadata JSONB
);

CREATE INDEX idx_dpa_entity_type_doorloop_id ON public.doorloop_pipeline_audit (entity_type, doorloop_id);
CREATE INDEX idx_dpa_internal_id ON public.doorloop_pipeline_audit (internal_id);
CREATE INDEX idx_dpa_pipeline_stage_status ON public.doorloop_pipeline_audit (pipeline_stage, status);
CREATE INDEX idx_dpa_event_timestamp ON public.doorloop_pipeline_audit (event_timestamp DESC);
CREATE INDEX idx_dpa_batch_id ON public.doorloop_pipeline_audit (batch_id);
CREATE INDEX idx_dpa_error_code ON public.doorloop_pipeline_audit (error_code);
