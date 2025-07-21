-- tables/kpi_functions.sql
CREATE OR REPLACE FUNCTION public.compute_and_store_all_kpis()
RETURNS VOID AS $$
DECLARE
    prop_count INTEGER;
    unit_cnt INTEGER;
    occupied_cnt INTEGER;
    delinquent_cnt INTEGER;
    vacant_cnt INTEGER;
    avg_rent NUMERIC;
BEGIN
    SELECT COUNT(*) INTO prop_count FROM doorloop_normalized_properties;
    SELECT COUNT(*) INTO unit_cnt FROM doorloop_normalized_units;
    SELECT COUNT(*) INTO occupied_cnt FROM doorloop_normalized_units WHERE status = 'Occupied';
    SELECT COUNT(*) INTO vacant_cnt FROM doorloop_normalized_units WHERE status = 'Vacant';
    SELECT COUNT(*) INTO delinquent_cnt FROM doorloop_normalized_tenants WHERE is_delinquent = true;
    SELECT AVG(market_rent) INTO avg_rent FROM doorloop_normalized_units;

    INSERT INTO public.kpi_summary (
        property_count, unit_count, occupied_units,
        occupancy_rate, delinquent_tenants, delinquency_rate,
        vacant_units, average_rent, computed_at
    )
    VALUES (
        prop_count, unit_cnt, occupied_cnt,
        CASE WHEN unit_cnt > 0 THEN ROUND((occupied_cnt::NUMERIC / unit_cnt) * 100, 2) ELSE 0 END,
        delinquent_cnt,
        CASE WHEN unit_cnt > 0 THEN ROUND((delinquent_cnt::NUMERIC / unit_cnt) * 100, 2) ELSE 0 END,
        vacant_cnt, avg_rent, NOW()
    );
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;
