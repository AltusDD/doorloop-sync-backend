from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.data_processing import clean_record

def sync_work_orders():
    entity = "work_orders"
    log_sync_start(entity)
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        all_records = client.get_all("/api/work-orders")
        normalized_records = []
        for item in all_records:
            record = {
                "doorloop_id": item.get("id"),
                "property_doorloop_id": item.get("property"),
                "unit_doorloop_id": item.get("unit"),
                "vendor_doorloop_id": item.get("vendor"),
                "status": item.get("status"),
                "priority": item.get("priority"),
                "title": item.get("title"),
                "description": item.get("description"),
                "scheduled_date": item.get("scheduledDate"),
                "due_date": item.get("dueDate"),
                "completion_date": item.get("completionDate"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(clean_record(record))
        supabase.insert_records("work_orders", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))
    except Exception as e:
        log_error(entity, str(e))
