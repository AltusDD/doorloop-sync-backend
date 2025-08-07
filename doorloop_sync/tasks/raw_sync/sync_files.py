from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.data_processing import clean_record

def sync_files():
    """
    Fetches all file metadata from DoorLoop and upserts it into the 'files' table.
    """
    entity = "files"
    log_sync_start(entity)
    
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        
        all_records = client.get_all("/api/files")
        
        if not all_records:
            log_sync_end(entity, 0)
            return

        normalized_records = []
        for item in all_records:
            linked_resource = item.get("linkedResource", {}) or {}
            record = {
                "doorloop_id": item.get("id"),
                "name": item.get("name"),
                "notes": item.get("notes"),
                "resource_id_dl": linked_resource.get("resourceId"),
                "resource_type": linked_resource.get("resourceType"),
                "size_bytes": item.get("size"),
                "mime_type": item.get("mimeType"),
                "download_url": item.get("downloadUrl"),
                "created_by_dl": item.get("createdBy"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(clean_record(record))

        supabase.insert_records("files", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))

    except Exception as e:
        log_error(entity, str(e))

# sync_files.py [silent tag]
