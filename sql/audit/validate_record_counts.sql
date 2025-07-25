DO $$
DECLARE
    raw_count INT;
    norm_count INT;
BEGIN
    SELECT COUNT(*) INTO raw_count FROM doorloop_raw_properties;
    SELECT COUNT(*) INTO norm_count FROM doorloop_normalized_properties;

    IF raw_count != norm_count THEN
        RAISE EXCEPTION 'Mismatch in property record counts: Raw=%, Normalized=%', raw_count, norm_count;
    END IF;
END $$;
