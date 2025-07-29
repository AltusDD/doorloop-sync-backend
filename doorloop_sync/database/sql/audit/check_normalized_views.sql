DO $$
DECLARE
    r_count INT;
BEGIN
    SELECT COUNT(*) INTO r_count FROM doorloop_normalized_properties;
    IF r_count = 0 THEN
        RAISE EXCEPTION 'No records in doorloop_normalized_properties!';
    END IF;
END $$;
