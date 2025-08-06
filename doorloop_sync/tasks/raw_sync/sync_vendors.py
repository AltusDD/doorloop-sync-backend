from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.data_processing import clean_record

def sync_vendors():
    entity = "vendors"
    log_sync_start(entity)
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        all_records = client.get_all("/api/vendors")
        normalized_records = []
        for item in all_records:
            record = {
                "doorloop_id": item.get("id"),
                "name": item.get("name"),
                "company_name": item.get("companyName"),
                "category": item.get("category"),
                "tax_id": item.get("taxId"),
                "active": item.get("active", True),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(clean_record(record))
        supabase.insert_records("vendors", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))
    except Exception as e:
        log_error(entity, str(e))
