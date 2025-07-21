#!/bin/bash
set -e

echo "Deploying Tables..."
for f in tables/*.sql; do
  echo "Deploying $f"
  psql "$SUPABASE_DB_URL" -f "$f"
done

echo "Deploying Views..."
for f in views/*.sql; do
  echo "Deploying $f"
  psql "$SUPABASE_DB_URL" -f "$f"
done

echo "Refreshing Materialized Views..."
psql "$SUPABASE_DB_URL" -c "REFRESH MATERIALIZED VIEW CONCURRENTLY public.get_full_kpi_by_property_view;"

echo "Done."