
-- Check if get_full views are pointing to normalized tables
SELECT 'get_full_properties_view' AS view_name, pg_get_viewdef('get_full_properties_view'::regclass, true) AS source
UNION ALL
SELECT 'get_full_units_view', pg_get_viewdef('get_full_units_view'::regclass, true)
UNION ALL
SELECT 'get_full_leases_view', pg_get_viewdef('get_full_leases_view'::regclass, true)
UNION ALL
SELECT 'get_full_tenants_view', pg_get_viewdef('get_full_tenants_view'::regclass, true)
UNION ALL
SELECT 'get_full_payments_view', pg_get_viewdef('get_full_payments_view'::regclass, true);
