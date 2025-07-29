DO $$
DECLARE
    broken INT;
BEGIN
    SELECT COUNT(*) INTO broken
    FROM doorloop_normalized_leases l
    LEFT JOIN doorloop_normalized_tenants t ON l.primary_tenant_id = t.id
    WHERE t.id IS NULL;

    IF broken > 0 THEN
        RAISE EXCEPTION 'Broken tenant foreign keys in leases: %', broken;
    END IF;
END $$;
