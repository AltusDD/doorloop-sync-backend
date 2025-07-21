#!/bin/bash
echo "🔁 Refreshing KPI Summary View..."
psql "$SUPABASE_DB_URL" -c "REFRESH MATERIALIZED VIEW CONCURRENTLY public.get_full_kpi_summary_view;"
