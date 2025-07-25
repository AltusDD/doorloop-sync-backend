CREATE MATERIALIZED VIEW IF NOT EXISTS public.dealroom_legal_summary_view AS
SELECT
    lc.id AS legal_case_id,
    lc.property_id,
    lc.tenant_id,
    lc.owner_id,
    lc.status,
    SUM(lb.total_amount) AS total_legal_billed,
    SUM(lb.paid_amount) AS total_legal_paid,
    SUM(lb.total_amount - lb.paid_amount) AS total_legal_outstanding
FROM public.legal_cases lc
LEFT JOIN public.legal_case_bills lb ON lb.case_id = lc.id
GROUP BY lc.id;