from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.utils_data import standardize_record

def sync_applications():
    """
    Fetches all rental applications from DoorLoop and upserts them into the
    'applications' table.
    """
    entity = "applications"
    log_sync_start(entity)
    
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        
        all_records = client.get_all("/api/rental-applications")
        
        if not all_records:
            log_sync_end(entity, 0)
            return

        normalized_records = []
        for item in all_records:
            record = {
                "doorloop_id": item.get("id"),
                "status": item.get("status"),
                "applicant_id_dl": item.get("applicant"),
                "property_id_dl": item.get("property"),
                "unit_id_dl": item.get("unit"),
                "lease_id_dl": item.get("lease"),
                "application_date": item.get("date"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(standardize_record(record))

        supabase.insert_records("applications", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))

    except Exception as e:
        log_error(entity, str(e))

# sync_applications.py [silent tag]
