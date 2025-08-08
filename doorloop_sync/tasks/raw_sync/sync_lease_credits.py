from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.utils_data import standardize_record

def sync_lease_credits():
    """
    Fetches all lease credits from DoorLoop and upserts them into the
    'lease_credits' table.
    """
    entity = "lease_credits"
    log_sync_start(entity)
    
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        
        all_records = client.get_all("/api/lease-credits")
        
        if not all_records:
            log_sync_end(entity, 0)
            return

        normalized_records = []
        for item in all_records:
            # Lease transactions have a nested 'lines' array
            primary_line = (item.get("lines") or [{}])[0]

            record = {
                "doorloop_id": item.get("id"),
                "amount_cents": int(float(primary_line.get("amount", 0)) * 100),
                "memo": item.get("memo") or primary_line.get("memo"),
                "date": item.get("date"),
                "lease_id_dl": item.get("lease"),
                "account_id_dl": primary_line.get("account"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(standardize_record(record))

        supabase.insert_records("lease_credits", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))

    except Exception as e:
        log_error(entity, str(e))

# sync_lease_credits.py [silent tag]
