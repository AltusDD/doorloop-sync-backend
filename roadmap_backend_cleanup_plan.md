# 🧹 Empire Repo Cleanup Plan

This file documents which files are allowed to remain in the `doorloop_sync_backend` repository. Anything **not listed below** is subject to removal.

---

## ✅ KEEP:

- deploy_all.sh
- tables/
- views/
- sql-proxy/
- scripts/
- .github/workflows/deploy_sql_proxy.yml
- .github/workflows/deploy-schema.yml
- .github/workflows/doorloop_sync.yml
- .github/workflows/run-cleanup.yml
- .github/workflows/validate-roadmap.yml
- README.md
- requirements.txt
- sync_all_doorloop_to_supabase.py

---

## 📌 Notes

- All `tables/` and `views/` files must follow proper naming: `doorloop_raw_*.sql`, `normalized_*.sql`, `get_full_*.sql`.
- Do not manually modify `doorloop_raw_*` or `normalized_*` tables outside the sync engine.
- All materialized views must be refreshed via sql-proxy only.