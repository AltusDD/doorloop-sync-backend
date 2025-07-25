CREATE TABLE IF NOT EXISTS public.legal_case_bills (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    case_id UUID REFERENCES public.legal_cases(id) ON DELETE CASCADE,
    invoice_number TEXT,
    invoice_date DATE,
    due_date DATE,
    total_amount NUMERIC(12, 2),
    paid_amount NUMERIC(12, 2) DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);