
# deploy_kpi_patch.sh
#!/bin/bash

set -e

psql $SUPABASE_DB_URL -f tables/kpi_summary.sql
psql $SUPABASE_DB_URL -f tables/compute_and_store_all_kpis.sql
psql $SUPABASE_DB_URL -f views/get_full_kpi_by_property_view.sql
