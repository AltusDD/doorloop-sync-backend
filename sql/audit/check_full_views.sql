-- Fails if any full view returns 0 rows
DO $$
DECLARE
    r_count INT;
BEGIN
    SELECT COUNT(*) INTO r_count FROM get_full_properties_view;
    IF r_count = 0 THEN
        RAISE EXCEPTION 'get_full_properties_view is empty!';
    END IF;
END $$;
