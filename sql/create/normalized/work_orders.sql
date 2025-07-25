CREATE TABLE IF NOT EXISTS work_orders (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    doorloop_id TEXT UNIQUE,
    property_id UUID REFERENCES properties(id),
    unit_id UUID REFERENCES units(id),
    vendor_id UUID REFERENCES vendors(id),
    status TEXT,
    description TEXT,
    scheduled_date DATE,
    completed_date DATE,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);
