CREATE OR REPLACE FUNCTION public.compute_and_store_all_kpis()
RETURNS VOID AS $$
BEGIN
    -- Example computation (stubbed for now)
    INSERT INTO public.kpi_summary (property_id, total_units, occupied_units, vacancy_rate, average_rent, delinquency_rate, turnover_rate)
    SELECT
        p.id,
        COUNT(u.id),
        COUNT(u.id) FILTER (WHERE u.status = 'occupied'),
        1 - (COUNT(u.id) FILTER (WHERE u.status = 'occupied')::NUMERIC / COUNT(u.id)),
        AVG(u.market_rent),
        0.0,  -- Placeholder for delinquency
        0.0   -- Placeholder for turnover
    FROM properties p
    LEFT JOIN units u ON u.property_id = p.id
    GROUP BY p.id;
END;
$$ LANGUAGE plpgsql SECURITY DEFINER;