-- views/get_full_kpi_summary_view.sql
CREATE MATERIALIZED VIEW IF NOT EXISTS public.get_full_kpi_summary_view AS
SELECT *
FROM public.kpi_summary
ORDER BY computed_at DESC;
