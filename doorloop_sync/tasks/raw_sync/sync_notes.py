from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.utils_data import standardize_record

def sync_notes():
    """
    Fetches all notes from DoorLoop and upserts them into the 'notes' table.
    """
    entity = "notes"
    log_sync_start(entity)
    
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        
        all_records = client.get_all("/api/notes")
        
        if not all_records:
            log_sync_end(entity, 0)
            return

        normalized_records = []
        for item in all_records:
            linked_resource = item.get("linkedResource", {}) or {}
            record = {
                "doorloop_id": item.get("id"),
                "title": item.get("title"),
                "body": item.get("body"),
                "resource_id_dl": linked_resource.get("resourceId"),
                "resource_type": linked_resource.get("resourceType"),
                "created_by_dl": item.get("createdBy"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(standardize_record(record))

        supabase.insert_records("notes", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))

    except Exception as e:
        log_error(entity, str(e))

# sync_notes.py [silent tag]
