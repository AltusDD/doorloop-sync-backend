from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.data_processing import clean_record

def sync_tasks():
    """
    Fetches ALL tasks (Work Orders, Tenant Requests, etc.) from the DoorLoop API
    and upserts them into the 'tasks' table in Supabase.
    """
    # RENAMED: The entity is now 'tasks' to reflect all types are being synced.
    entity = "tasks"
    log_sync_start(entity)
    
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        
        endpoint = "/api/tasks"
        
        # REMOVED: The parameter that filtered for only work orders is now gone.
        all_records = client.get_all(endpoint)
        
        if not all_records:
            log_sync_end(entity, 0)
            return

        normalized_records = []
        for item in all_records:
            # This mapping aligns with your 'tasks' table schema
            record = {
                "doorloop_id": item.get("id"),
                "type": item.get("type"), # Will now include all types
                "subject": item.get("subject"),
                "description": item.get("description"),
                "status": item.get("status"),
                "priority": item.get("priority"),
                "due_date": item.get("dueDate"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
                "property_id_dl": item.get("property"), # DoorLoop ID, requires linking
                "unit_id_dl": item.get("unit"), # DoorLoop ID, requires linking
                "tenant_id_dl": item.get("requestedByTenant"), # DoorLoop ID
                "vendor_id_dl": item.get("workOrder", {}).get("assignedToVendor"), # Nested field
                "assigned_user_id_dl": (item.get("assignedToUsers") or [None])[0], # Handle list
            }
            normalized_records.append(clean_record(record))

        # The target table remains 'tasks'
        supabase.insert_records("tasks", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))

    except Exception as e:
        log_error(entity, str(e))

# sync_work_orders.py [silent tag]
# Empire grade sync task
