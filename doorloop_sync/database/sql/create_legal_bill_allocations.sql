CREATE TABLE IF NOT EXISTS public.legal_bill_allocations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bill_id UUID REFERENCES public.legal_case_bills(id) ON DELETE CASCADE,
    property_id TEXT REFERENCES public.doorloop_normalized_properties(id),
    lease_id TEXT REFERENCES public.doorloop_normalized_leases(id),
    tenant_id TEXT REFERENCES public.doorloop_normalized_tenants(id),
    owner_id UUID REFERENCES public.doorloop_normalized_owners(id),
    amount NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW(),
    CONSTRAINT chk_one_doorloop_entity_per_allocation CHECK (
        (property_id IS NOT NULL)::int +
        (lease_id IS NOT NULL)::int +
        (tenant_id IS NOT NULL)::int +
        (owner_id IS NOT NULL)::int = 1
    )
);