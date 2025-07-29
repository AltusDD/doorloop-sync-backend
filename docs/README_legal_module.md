# Empire Legal Tracker Setup

1. Run `legal_lifecycle_tables.sql` in Supabase SQL editor.
2. Deploy `sync_legal_cases_from_excel.py` via GitHub → Azure Functions.
3. Run `materialized_views.sql` and `prevent_finalized_edits.sql` for views and locking.
4. Use this README as the module reference.

Make sure Supabase foreign keys match your normalized DoorLoop schema (text/uuid).