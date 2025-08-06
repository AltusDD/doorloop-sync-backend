from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.data_processing import clean_record

def sync_units():
    entity = "units"
    log_sync_start(entity)
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        all_records = client.get_all("/api/units")
        normalized_records = []
        for item in all_records:
            record = {
                "doorloop_id": item.get("id"),
                "unit_number": item.get("name"),
                "beds": item.get("beds"),
                "baths": item.get("baths"),
                "sq_ft": item.get("size"),
                "rent_amount_cents": int(float(item.get("marketRent", 0)) * 100),
                "active": item.get("active", True),
                "property_doorloop_id": item.get("property"),
                "description": item.get("description"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(clean_record(record))
        supabase.insert_records("units", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))
    except Exception as e:
        log_error(entity, str(e))
