from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.utils_data import standardize_record

def sync_tasks():
    """
    Fetches ALL tasks (Work Orders, Tenant Requests, etc.) from DoorLoop
    and upserts them into the 'tasks' table.
    """
    entity = "tasks"
    log_sync_start(entity)
    
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        
        all_records = client.get_all("/api/tasks")
        
        if not all_records:
            log_sync_end(entity, 0)
            return

        normalized_records = []
        for item in all_records:
            # Safely access nested work order data
            work_order_data = item.get("workOrder", {}) or {}
            
            record = {
                "doorloop_id": item.get("id"),
                "type": item.get("type"),
                "subject": item.get("subject"),
                "description": item.get("description"),
                "status": item.get("status"),
                "priority": item.get("priority"),
                "due_date": item.get("dueDate"),
                "property_id_dl": item.get("property"),
                "unit_id_dl": item.get("unit"),
                "tenant_id_dl": item.get("requestedByTenant"),
                "owner_id_dl": item.get("requestedByOwner"),
                "user_id_dl": item.get("requestedByUser"),
                "vendor_id_dl": work_order_data.get("assignedToVendor"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(standardize_record(record))

        supabase.insert_records("tasks", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))

    except Exception as e:
        log_error(entity, str(e))

# sync_tasks.py [silent tag]
