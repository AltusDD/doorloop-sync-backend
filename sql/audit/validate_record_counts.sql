
-- Audit record counts between raw and normalized tables
SELECT
    'properties' AS entity,
    (SELECT COUNT(*) FROM doorloop_raw_properties) AS raw_count,
    (SELECT COUNT(*) FROM doorloop_normalized_properties) AS normalized_count
UNION
SELECT
    'units',
    (SELECT COUNT(*) FROM doorloop_raw_units),
    (SELECT COUNT(*) FROM doorloop_normalized_units)
UNION
SELECT
    'leases',
    (SELECT COUNT(*) FROM doorloop_raw_leases),
    (SELECT COUNT(*) FROM doorloop_normalized_leases)
UNION
SELECT
    'tenants',
    (SELECT COUNT(*) FROM doorloop_raw_tenants),
    (SELECT COUNT(*) FROM doorloop_normalized_tenants)
UNION
SELECT
    'owners',
    (SELECT COUNT(*) FROM doorloop_raw_owners),
    (SELECT COUNT(*) FROM doorloop_normalized_owners)
UNION
SELECT
    'vendors',
    (SELECT COUNT(*) FROM doorloop_raw_vendors),
    (SELECT COUNT(*) FROM doorloop_normalized_vendors)
UNION
SELECT
    'payments',
    (SELECT COUNT(*) FROM doorloop_raw_payments),
    (SELECT COUNT(*) FROM doorloop_normalized_payments)
UNION
SELECT
    'lease_payments',
    (SELECT COUNT(*) FROM doorloop_raw_lease_payments),
    (SELECT COUNT(*) FROM doorloop_normalized_lease_payments)
UNION
SELECT
    'work_orders',
    (SELECT COUNT(*) FROM doorloop_raw_work_orders),
    (SELECT COUNT(*) FROM doorloop_normalized_work_orders);
