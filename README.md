# 🏛️ Altus Empire Backend Sync System

This repository powers the **Altus Empire Command Center** backend, managing the full data pipeline from **DoorLoop → Supabase**. It includes structured ingestion, normalization, KPI computation, auditing, legal module integration, and sync retry infrastructure.

---

## 📦 System Architecture

### 1. Raw Data Ingestion
- Source: DoorLoop API (v2)
- Location: `tables/doorloop_raw_*.sql`
- Status: ✅ Live
- Triggered by: `sync_all_doorloop_to_supabase.py` via GitHub Actions

### 2. Normalization Layer (Phase 2)
- Converts DoorLoop raw → normalized schema (UUID-based, joinable)
- Location: `scripts/normalized_*.sql` and `sync_pipeline/normalize_*.py`
- Orchestrated via: `normalize_and_patch_all.py`
- Status: 🟡 Partial (see below)

### 3. KPI + Views (Phase 3+)
- Materialized + full reporting views: `views/get_full_*.sql`
- KPI Computation: `tables/kpi_summary.sql`, `tables/compute_and_store_all_kpis.sql`
- Triggered by: `cli/compute_kpis.py` and GitHub runner `run_kpi_sync.yml`

### 4. Legal Module
- Setup via: `README_legal_module.md`
- Supports: Excel sync, Dropbox uploads, multi-phase legal lifecycle tracking

---

## ✅ Deployment Instructions

### 🛠 Initial Setup

```bash
# Install requirements
pip install -r requirements.txt

# Sync raw DoorLoop data into Supabase
python sync_all_doorloop_to_supabase.py

# Normalize and patch all entities
python normalize_and_patch_all.py

# Compute KPIs from normalized tables
python cli/compute_kpis.py
```

### 🌀 Optional Commands

```bash
# Retry failed records from DLQ (if audit enabled)
python cli/retry_dlq_records.py

# Refresh materialized views (backend-only)
bash scripts/refresh_all_materialized_views.sh
```

---

## 🧹 Repo Cleanup Enforcement

This repo is governed by: `roadmap_backend_cleanup_plan.md`  
All files must follow canonical naming and architecture. Invalid or untracked files will be removed by automation.

Key policies:
- Only keep files listed in `roadmap_backend_cleanup_plan.md`
- Use `validate_against_roadmap.sh` before PRs or deploys.
- All workflows must follow emoji-tagged filenames (e.g., `📦_normalize_data.yml`).

---

## 📊 Normalization Phase Coverage (As of July 2025)

| Entity            | Raw Table              | Normalized SQL         | Python Normalizer                   | Status   |
|-------------------|------------------------|------------------------|-------------------------------------|----------|
| Properties        | ✅ `doorloop_raw_properties` | ✅ `normalized_properties.sql` | ✅ `normalize_properties.py`     | ✅ Done   |
| Units             | ✅ `doorloop_raw_units`     | ✅ `normalized_units.sql`     | ✅ `normalize_units.py`           | ✅ Done   |
| Tenants           | ✅ `doorloop_raw_tenants`   | ✅ `normalized_tenants.sql`   | ✅ `normalize_tenants.py`         | ✅ Done   |
| Owners            | ✅ `doorloop_raw_owners`    | ✅ `normalized_owners.sql`    | ✅ `normalize_owners.py`          | ✅ Done   |
| Leases            | ✅ `doorloop_raw_leases`    | ✅ `normalized_leases.sql`    | ✅ `normalize_leases.py`          | ✅ Done   |
| Lease Payments    | ✅ `doorloop_raw_lease_payments` | 🔲 (Pending)        | 🔲 (Pending)                       | 🚧 Missing |
| Tasks             | ✅ `doorloop_raw_tasks`     | 🔲 (Pending)               | 🔲 (Pending)                       | 🚧 Missing |
| Vendors           | ✅ `doorloop_raw_vendors`   | 🔲 (Pending)               | 🔲 (Pending)                       | 🚧 Missing |
| Work Orders       | ✅ `doorloop_raw_work_orders` | 🔲 (Pending)               | 🔲 (Pending)                       | 🚧 Missing |
| Users             | ✅ `doorloop_raw_users`     | 🔲 (Pending)               | 🔲 (Pending)                       | 🚧 Missing |

> ✅ = Completed and verified  
> 🔲 = Normalizer or view pending  
> 🚧 = Needs immediate attention in Phase 2 wrap-up

---

## 📡 Health & DLQ Monitoring

- Failed record handling: `doorloop_error_records`, DLQ pipelines, retry CLI
- Retry logic: `cli/move_errors_to_dlq.py`, `cli/retry_dlq_records.py`
- Auditing: `sql/schema/audit_table.sql`, `create_dlq_retry_dashboard.sql`

---

## 📚 References

- 🔗 DoorLoop API Reference: [https://api.doorloop.com/reference/authentication](https://api.doorloop.com/reference/authentication)
- 📄 Relationship Map: `DoorLoop API - Relationships.docx`
- 📦 Cleanup Plan: `roadmap_backend_cleanup_plan.md`
- ⚖️ Legal Module Setup: `README_legal_module.md`

---

Empire execution only.  
Altus-grade integrity guaranteed.
