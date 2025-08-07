from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.data_processing import clean_record

def sync_owners():
    """
    Fetches all owners from DoorLoop and upserts them into the 'owners' table.
    """
    entity = "owners"
    log_sync_start(entity)
    
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        
        all_records = client.get_all("/api/owners")
        
        if not all_records:
            log_sync_end(entity, 0)
            return

        normalized_records = []
        for item in all_records:
            record = {
                "doorloop_id": item.get("id"),
                "first_name": item.get("firstName"),
                "last_name": item.get("lastName"),
                "full_name": item.get("fullName"),
                "display_name": item.get("name"),
                "company_name": item.get("companyName"),
                "notes": item.get("notes"),
                "active": item.get("active", True),
                "management_start_date": item.get("managementStartDate"),
                "management_end_date": item.get("managementEndDate"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(clean_record(record))

        supabase.insert_records("owners", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))

    except Exception as e:
        log_error(entity, str(e))

# sync_owners.py [silent tag]
