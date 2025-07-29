
-- Filename: sql/refresh_all_materialized_views_function.sql
-- Creates a secure RPC endpoint in Supabase

CREATE OR REPLACE FUNCTION refresh_all_materialized_views()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
  REFRESH MATERIALIZED VIEW private.get_full_properties_view;
  REFRESH MATERIALIZED VIEW private.get_full_units_view;
  REFRESH MATERIALIZED VIEW private.get_full_leases_view;
  REFRESH MATERIALIZED VIEW private.get_full_tenants_view;
  REFRESH MATERIALIZED VIEW private.get_full_owners_view;
END;
$$;
