CREATE TABLE IF NOT EXISTS normalized_tasks (
    id UUID PRIMARY KEY,
    title TEXT,
    description TEXT,
    status TEXT,
    due_date DATE,
    assigned_to UUID REFERENCES normalized_users(id) ON DELETE SET NULL,
    related_unit_id UUID REFERENCES normalized_units(id) ON DELETE SET NULL,
    related_lease_id UUID REFERENCES normalized_leases(id) ON DELETE SET NULL,
    created_at TIMESTAMP DEFAULT now(),
    updated_at TIMESTAMP DEFAULT now()
);
