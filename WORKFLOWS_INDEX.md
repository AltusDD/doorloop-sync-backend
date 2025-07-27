
# 🧠 Altus Empire Command Center – GitHub Actions Workflow Index

This file documents all active workflows in `.github/workflows/` that are part of the Empire-grade backend pipeline. Each workflow includes its purpose, trigger type, and dependency notes.

---

## 🚀 Core Pipeline Workflows (Essential)

### `🚀_sync_doorloop_to_supabase.yml`
- **Purpose:** Syncs raw data from DoorLoop API into Supabase raw tables.
- **Trigger:** Manual or scheduled
- **Depends On:** Secrets `SUPABASE_URL`, `SUPABASE_SERVICE_ROLE_KEY`

### `🚀_normalize_and_patch_data.yml`
- **Purpose:** Normalizes raw data into structured tables and patches records.
- **Trigger:** Manual or on sync success
- **Depends On:** Output from sync workflow

### `📈_run_kpi_pipeline.yml`
- **Purpose:** Computes core KPIs and stores them in Supabase.
- **Trigger:** After normalization
- **Depends On:** Normalized tables

### `♻️_refresh_kpi_view.yml`
- **Purpose:** Refreshes KPI materialized views for frontend.
- **Trigger:** Auto-scheduled or post-KPI-pipeline
- **Depends On:** KPI pipeline

---

## 📂 Excel Legal Import System

### `🚀_deploy_excel_upload_engine.yml`
- **Purpose:** Uploads Excel data to `legal_cases_import_staging`
- **Trigger:** Manual with file upload
- **Depends On:** Excel format matching `legal_import_pipeline.md`

### `🚀_deploy_excel_staging_validator.yml`
- **Purpose:** Validates and resolves foreign keys in staging table.
- **Trigger:** On Excel upload complete
- **Depends On:** `legal_cases_import_staging`

### `🚀_sync_legal_cases_manual.yml`
- **Purpose:** Moves validated staged cases into `legal_cases`
- **Trigger:** Manual
- **Depends On:** Validated staging data

### `🚀_sync_legal_cases_auto.yml`
- **Purpose:** Auto-scheduled legal case sync
- **Trigger:** Cron
- **Depends On:** New records in staging

---

## 🧰 Maintenance & Debugging

### `🔧_install_dependencies_hardened.yml`
- **Purpose:** Hard-install psycopg2-binary and ensure all pip dependencies are live
- **Trigger:** Manual debug
- **Notes:** Use if dependency bugs persist

### `✅_validate_against_roadmap.yml`
- **Purpose:** Ensures repo files match `roadmap_backend_cleanup_plan.md`
- **Trigger:** Manual (optional)
- **Notes:** Archive if no longer enforcing roadmap

---

## 📁 Archived/Legacy Workflows (now moved to `.github/workflows/ARCHIVE/`)

- `ci.yml`
- `deploy_sql_proxy.yml`
- `lockdown_phase_4.yml`
- `refresh_all_views.yml`
- `run_kpi_sync.yml`
- `validate_roadmap.yml` (if archived)

---

**Maintainer Note:** All active workflows must be marked with an emoji prefix (`🚀`, `📈`, `✅`, `🔧`, etc.) to distinguish core scripts from utility/debug/archive files.
