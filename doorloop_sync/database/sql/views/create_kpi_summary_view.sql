CREATE TABLE IF NOT EXISTS kpi_summary (
    key TEXT PRIMARY KEY,
    value TEXT,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
