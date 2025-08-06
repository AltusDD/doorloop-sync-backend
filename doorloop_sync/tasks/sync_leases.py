from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.data_processing import clean_record

def sync_leases():
    entity = "leases"
    log_sync_start(entity)
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        all_records = client.get_all("/api/leases")
        normalized_records = []
        for item in all_records:
            record = {
                "doorloop_id": item.get("id"),
                "unit_doorloop_id": item.get("unit"),
                "property_doorloop_id": item.get("property"),
                "start_date": item.get("startDate"),
                "end_date": item.get("endDate"),
                "status": item.get("status"),
                "deposit_cents": int(float(item.get("deposit", 0)) * 100),
                "rent_amount_cents": int(float(item.get("rentAmount", 0)) * 100),
                "is_active": item.get("active", True),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(clean_record(record))
        supabase.insert_records("leases", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))
    except Exception as e:
        log_error(entity, str(e))
