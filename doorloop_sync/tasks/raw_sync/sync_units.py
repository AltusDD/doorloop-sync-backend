from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.data_processing import clean_record

def sync_units():
    """
    Fetches all units from DoorLoop and upserts them into the 'units' table,
    linking them to their parent properties.
    """
    entity = "units"
    log_sync_start(entity)
    
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        
        all_records = client.get_all("/api/units")
        
        if not all_records:
            log_sync_end(entity, 0)
            return

        normalized_records = []
        for item in all_records:
            record = {
                "doorloop_id": item.get("id"),
                "unit_number": item.get("name"),
                "beds": item.get("beds"),
                "baths": item.get("baths"),
                "sq_ft": item.get("size"),
                "active": item.get("active", True),
                "rent_amount_cents": int(float(item.get("marketRent", 0)) * 100),
                "description": item.get("description"),
                "property_id_dl": item.get("property"), # Store the DoorLoop property ID for linking
                "floor_plan": item.get("floorPlan"),
                "is_rentable": item.get("isRentable"),
                "last_renovated": item.get("lastRenovated"),
                "unit_condition": item.get("condition"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(clean_record(record))

        supabase.insert_records("units", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))

    except Exception as e:
        log_error(entity, str(e))

# sync_units.py [silent tag]
