from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.data_processing import clean_record

def sync_activity_logs():
    """
    Fetches all activity logs from DoorLoop and upserts them into the
    'activity_logs' table.
    """
    entity = "activity_logs"
    log_sync_start(entity)
    
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        
        all_records = client.get_all("/api/activity-logs")
        
        if not all_records:
            log_sync_end(entity, 0)
            return

        normalized_records = []
        for item in all_records:
            record = {
                "doorloop_id": item.get("id"),
                "action": item.get("action"),
                "description": item.get("description"),
                "user_id_dl": item.get("user"),
                "resource_id_dl": item.get("resourceId"),
                "resource_type": item.get("resourceType"),
                "created_at": item.get("createdAt"),
            }
            normalized_records.append(clean_record(record))

        supabase.insert_records("activity_logs", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))

    except Exception as e:
        log_error(entity, str(e))

# sync_activity_logs.py [silent tag]
