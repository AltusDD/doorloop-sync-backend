
-- compute_and_store_all_kpis.sql
CREATE OR REPLACE FUNCTION public.compute_and_store_all_kpis()
RETURNS VOID LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    -- Clear existing KPIs
    DELETE FROM public.kpi_summary;

    -- Example KPI calculation logic (simplified)
    INSERT INTO public.kpi_summary (property_id, units_count, occupied_units, occupancy_rate, total_rent, delinquent_rent, vacancy_cost)
    SELECT
        p.id,
        COUNT(u.id) AS units_count,
        COUNT(u.id) FILTER (WHERE u.status = 'occupied') AS occupied_units,
        ROUND(COUNT(u.id) FILTER (WHERE u.status = 'occupied') * 100.0 / NULLIF(COUNT(u.id), 0), 2) AS occupancy_rate,
        SUM(u.market_rent) AS total_rent,
        SUM(CASE WHEN u.status = 'delinquent' THEN u.market_rent ELSE 0 END) AS delinquent_rent,
        SUM(CASE WHEN u.status = 'vacant' THEN u.market_rent ELSE 0 END) AS vacancy_cost
    FROM
        doorloop_normalized_properties p
        LEFT JOIN doorloop_normalized_units u ON p.id = u.property_id
    GROUP BY
        p.id;
END;
$$;
