# Empire Sync Fix Bundle

## Steps to Fix and Normalize Your Sync Environment

### 1. 🚀 Deploy Normalized Tables

Run this to create all normalized tables with correct structure in Supabase:

```bash
chmod +x deploy-database.sh
SUPABASE_DB_URL=postgres://your_url_here bash deploy-database.sh
```

Make sure your `SUPABASE_DB_URL` is set or exported.

---

### 2. 🧼 Normalize All Raw → Normalized

Run this:

```bash
python normalize_and_patch_all.py
```

This will:
- Pull data from raw tables like `doorloop_raw_properties`
- Push normalized clean rows to `doorloop_normalized_*` tables

---

### 3. 📊 Refresh Materialized KPI View

```bash
curl -X POST "https://<your>.supabase.co/functions/v1/sql-proxy" \
  -H "Authorization: Bearer $SUPABASE_SERVICE_ROLE_KEY" \
  -H "Content-Type: application/json" \
  -d '{ "sql": "REFRESH MATERIALIZED VIEW CONCURRENTLY get_full_kpi_by_property_view;" }'
```

---

✅ After this, you will have fully deployed and normalized data syncs across all core tables.