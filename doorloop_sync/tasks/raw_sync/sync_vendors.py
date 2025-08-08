import logging
# CORRECTED: Use the full, absolute path to the client module
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.utils_data import standardize_record

def sync_vendors():
    """
    Fetches all vendors from DoorLoop and upserts them into the 'vendors' table.
    """
    entity = "vendors"
    log_sync_start(entity)
    
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        
        all_records = client.get_all("/api/vendors")
        
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
                "balance_cents": int(float(item.get("balance", 0)) * 100),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(standardize_record(record))

        supabase.insert_records("vendors", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))

    except Exception as e:
        log_error(entity, str(e))

# sync_vendors.py [silent tag]
