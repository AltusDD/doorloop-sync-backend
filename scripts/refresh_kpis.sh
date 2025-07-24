#!/bin/bash
echo "🔁 Refreshing KPI Materialized View..."
curl -X POST "https://ssexobxvtuxwnblwplzh.supabase.co/functions/v1/sql-proxy"   -H "Authorization: $SQL_PROXY_SECRET"   -H "Content-Type: application/json"   -d '{ "sql": "REFRESH MATERIALIZED VIEW CONCURRENTLY get_full_kpi_summary_view;" }'
