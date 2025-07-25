# ✅ Roadmap Backend Cleanup Plan

This roadmap file lists all files that are approved to remain in the repo.
Any file not listed here will trigger a CI validation error.

---

## 📁 .github/workflows/

KEEP: .github/workflows/deploy_excel_staging_validator.yml
KEEP: .github/workflows/run_kpi_sync.yml
KEEP: .github/workflows/🧠_sync_legal_cases_auto.yml
KEEP: .github/workflows/🔒_lockdown_phase_4.yml
KEEP: .github/workflows/📦_deploy_sql_proxy.yml
KEEP: .github/workflows/🧼_cleanup.yml
KEEP: .github/workflows/📈_normalize_and_patch_data.yml
KEEP: .github/workflows/🧠_sync_legal_cases.yml
KEEP: .github/workflows/🧠_sync_legal_cases_manual.yml
KEEP: .github/workflows/🧠_deploy_excel_upload_engine.yml

## 📁 Root Project Files

KEEP: .env.example
KEEP: .gitignore
KEEP: README.md
KEEP: README_legal_module.md
KEEP: roadmap_backend_cleanup_plan.md
KEEP: requirements.txt
KEEP: validate_against_roadmap.sh

## 📁 Scripts & CLI Tools

KEEP: cli/compute_kpis.py
KEEP: cli/move_errors_to_dlq.py
KEEP: cli/push_mismatches_to_dlq.py
KEEP: cli/retry_dlq_records.py
KEEP: cli/retry_failed_record.py

## 📁 Normalize and Sync

KEEP: normalize_and_patch_all.py
KEEP: sync_all_doorloop_to_supabase.py
KEEP: sync_missing_doorloop_categories.py
KEEP: sync_normalized_data.py

## 📁 Normalizers

KEEP: normalizers/normalize_lease_payments.py
KEEP: normalizers/normalize_leases.py
KEEP: normalizers/normalize_owners.py
KEEP: normalizers/normalize_payments.py
KEEP: normalizers/normalize_properties.py
KEEP: normalizers/normalize_tenants.py
KEEP: normalizers/normalize_units.py
KEEP: normalizers/normalize_vendors.py
KEEP: normalizers/normalize_work_orders.py

## 📁 SQL Scripts

KEEP: deploy_normalized_views.py
KEEP: generate_normalized_views.py
KEEP: migrate_schema.py
KEEP: migrate_schema_and_refresh.py
KEEP: refresh_all_materialized_views.py
KEEP: fix_all_normalized_views.sh
KEEP: fix_deployment.sh

## 📁 Supabase Functions & Helpers

KEEP: supabase/functions/sql-proxy/index.ts
KEEP: supabase_client.py
KEEP: supabase_ingest_client.py
KEEP: supabase_schema_manager.py

## 📁 Azure Functions

KEEP: azure_functions/health_alerts/__init__.py
KEEP: azure_functions/health_alerts/function.json
KEEP: azure_functions/health_alerts/requirements.txt
KEEP: azure_functions/health_alerts/README.md

## 📁 Views, Tables, and Materialized Views

KEEP: views/get_full/get_full_properties_view.sql
KEEP: views/get_full/get_full_units_view.sql
KEEP: views/get_full/get_full_leases_view.sql
KEEP: views/get_full/get_full_tenants_view.sql
KEEP: views/get_full/get_full_owners_view.sql
KEEP: views/get_full/get_full_vendors_view.sql
KEEP: views/get_full/get_full_work_orders_view.sql
KEEP: views/sql/views/create_kpi_summary_view.sql
KEEP: scripts/refresh_kpis.sh
KEEP: scripts/refresh_all_materialized_views.sql
