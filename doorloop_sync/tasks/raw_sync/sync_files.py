from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.data_processing import clean_record

def sync_files():
    entity = "files"
    log_sync_start(entity)
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        all_records = client.get_all("/api/files")
        normalized_records = []
        for item in all_records:
            record = {
                "doorloop_id": item.get("id"),
                "filename": item.get("fileName"),
                "entity_type": item.get("entityType"),
                "entity_id": item.get("entityId"),
                "mime_type": item.get("mimeType"),
                "size_bytes": item.get("size"),
                "url": item.get("url"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(clean_record(record))
        supabase.insert_records("files", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))
    except Exception as e:
        log_error(entity, str(e))
