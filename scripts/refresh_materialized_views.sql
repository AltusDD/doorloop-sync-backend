-- Refresh all materialized views in the proper dependency order
REFRESH MATERIALIZED VIEW CONCURRENTLY get_full_properties_view;
REFRESH MATERIALIZED VIEW CONCURRENTLY get_full_units_view;
REFRESH MATERIALIZED VIEW CONCURRENTLY get_full_leases_view;
REFRESH MATERIALIZED VIEW CONCURRENTLY get_full_tenants_view;
REFRESH MATERIALIZED VIEW CONCURRENTLY get_full_work_orders_view;
