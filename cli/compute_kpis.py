import os
import psycopg2
from psycopg2.extras import RealDictCursor

def compute_kpis():
    conn = psycopg2.connect(os.getenv("SUPABASE_DIRECT_DB_URL"))
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # Compute KPIs from normalized tables
    cur.execute("""
        INSERT INTO kpi_summary (key, value)
        VALUES 
            ('total_properties', (SELECT COUNT(*) FROM doorloop_normalized_properties)),
            ('total_units', (SELECT COUNT(*) FROM doorloop_normalized_units)),
            ('occupied_units', (SELECT COUNT(*) FROM doorloop_normalized_units WHERE occupancy_status = 'occupied')),
            ('active_leases', (SELECT COUNT(*) FROM doorloop_normalized_leases WHERE status = 'active')),
            ('lease_expiring_30_days', (
                SELECT COUNT(*) FROM doorloop_normalized_leases 
                WHERE status = 'active' 
                  AND end_date BETWEEN now() AND now() + interval '30 days')),
            ('delinquent_balance_total', (
                SELECT COALESCE(SUM(balance), 0) FROM doorloop_normalized_leases WHERE balance > 0)),
            ('avg_unit_rent', (
                SELECT ROUND(AVG(market_rent)::numeric, 2) FROM doorloop_normalized_units WHERE market_rent > 0))
        ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value;
    """)

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    compute_kpis()
