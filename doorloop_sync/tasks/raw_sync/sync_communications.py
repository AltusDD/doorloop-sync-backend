from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.utils_data import standardize_record

def sync_communications():
    """
    Fetches all communication logs from DoorLoop and upserts them into the
    'communications' table.
    """
    entity = "communications"
    log_sync_start(entity)
    
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        
        all_records = client.get_all("/api/communications")
        
        if not all_records:
            log_sync_end(entity, 0)
            return

        normalized_records = []
        for item in all_records:
            record = {
                "doorloop_id": item.get("id"),
                "subject": item.get("subject"),
                "body_preview": item.get("bodyPreview"),
                "sent_at": item.get("sentAt"),
                "type": item.get("type"), # e.g., EMAIL, PHONE, TEXT
                "status": item.get("status"),
                "thread_id": item.get("threadId"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(standardize_record(record))

        supabase.insert_records("communications", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))

    except Exception as e:
        log_error(entity, str(e))

# sync_communications.py [silent tag]
