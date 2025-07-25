
CREATE TABLE IF NOT EXISTS doorloop_error_records (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  entity_type TEXT NOT NULL,
  doorloop_id TEXT,
  error_payload JSONB NOT NULL,
  error_message TEXT,
  attempted_at TIMESTAMPTZ DEFAULT now(),
  retry_attempts INT DEFAULT 0
);
