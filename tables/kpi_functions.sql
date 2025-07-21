CREATE OR REPLACE FUNCTION public.compute_and_store_all_kpis()
RETURNS void LANGUAGE plpgsql SECURITY DEFINER AS $$
BEGIN
    DELETE FROM public.kpi_summary;
    INSERT INTO public.kpi_summary (property_id, unit_count, occupied_units, occupancy_rate, total_rent, vacancy_cost, delinquency_count)
    SELECT 
        p.id AS property_id,
        COUNT(u.id) AS unit_count,
        COUNT(u.id) FILTER (WHERE u.status = 'occupied') AS occupied_units,
        CASE WHEN COUNT(u.id) > 0 THEN 
            ROUND(COUNT(u.id) FILTER (WHERE u.status = 'occupied') * 100.0 / COUNT(u.id), 2)
        ELSE 0 END AS occupancy_rate,
        SUM(u.market_rent) AS total_rent,
        SUM(CASE WHEN u.status = 'vacant' THEN u.market_rent ELSE 0 END) AS vacancy_cost,
        COUNT(lp.id) FILTER (WHERE lp.amount_due > 0) AS delinquency_count
    FROM public.doorloop_normalized_properties p
    LEFT JOIN public.doorloop_normalized_units u ON u.property_id = p.id
    LEFT JOIN public.doorloop_normalized_lease_payments lp ON lp.unit_id = u.id
    GROUP BY p.id;
END;
$$;
