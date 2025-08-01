Run python -m doorloop_sync.orchestrator
2025-08-01 16:46:19,247 - INFO - 🚀 Starting full DoorLoop sync pipeline
2025-08-01 16:46:19,247 - INFO - ✅ DoorLoopClient initialized. Using validated BASE URL: https://app.doorloop.com/api/
2025-08-01 16:46:19,307 - INFO - ✅ SupabaseIngestClient initialized.
2025-08-01 16:46:19,308 - INFO - 🚀 Syncing properties from DoorLoop...
2025-08-01 16:46:19,308 - INFO - [AUDIT] sync_start: Beginning property sync | Entity: PropertySyncService
2025-08-01 16:46:19,308 - INFO - 📡 Fetching all records from properties...
2025-08-01 16:46:19,457 - INFO -   - Fetched page 1 (50 records). Total fetched so far: 50/184
2025-08-01 16:46:19,803 - INFO -   - Fetched page 2 (50 records). Total fetched so far: 100/184
2025-08-01 16:46:20,398 - INFO -   - Fetched page 3 (50 records). Total fetched so far: 150/184
2025-08-01 16:46:20,723 - INFO -   - Fetched page 4 (50 records). Total fetched so far: 200/184
2025-08-01 16:46:20,724 - INFO - ✅ Finished fetching from /properties. Total records: 200
2025-08-01 16:46:20,724 - INFO - 📥 Retrieved 200 properties
2025-08-01 16:46:20,724 - INFO - [AUDIT] sync_data_received: Retrieved 200 properties | Entity: PropertySyncService
2025-08-01 16:46:20,724 - INFO - 🔁 Normalized 200 properties
2025-08-01 16:46:20,724 - INFO - [AUDIT] sync_normalized: Normalized 200 properties | Entity: PropertySyncService
2025-08-01 16:46:20,724 - ERROR - ❌ Error syncing PropertySyncService
Traceback (most recent call last):
  File "/home/runner/work/doorloop-sync-backend/doorloop-sync-backend/doorloop_sync/services/property_service.py", line 30, in sync
    self.supabase.upsert(table="doorloop_raw_properties", data=normalized)
TypeError: SupabaseIngestClient.upsert() got an unexpected keyword argument 'table'
2025-08-01 16:46:20,725 - INFO - [AUDIT] sync_error: SupabaseIngestClient.upsert() got an unexpected keyword argument 'table' | Entity: PropertySyncService
2025-08-01 16:46:20,725 - INFO - ✅ Finished entity syncing. Computing KPIs...
✅ Syncing UnitSyncService... (stub logic)
✅ Syncing OwnerSyncService... (stub logic)
✅ Syncing LeaseSyncService... (stub logic)
✅ Syncing TenantSyncService... (stub logic)
✅ Syncing PaymentSyncService... (stub logic)
📊 Computing and storing KPIs... (stub)