from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.data_processing import clean_record

def sync_inspections():
    """
    Fetches all inspections from DoorLoop and upserts them into the
    'inspections' table.
    """
    entity = "inspections"
    log_sync_start(entity)
    
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        
        all_records = client.get_all("/api/inspections")
        
        if not all_records:
            log_sync_end(entity, 0)
            return

        normalized_records = []
        for item in all_records:
            record = {
                "doorloop_id": item.get("id"),
                "status": item.get("status"),
                "type": item.get("type"),
                "inspection_date": item.get("inspectionDate"),
                "property_id_dl": item.get("property"),
                "unit_id_dl": item.get("unit"),
                "inspector_id_dl": item.get("inspector"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(clean_record(record))

        supabase.insert_records("inspections", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))

    except Exception as e:
        log_error(entity, str(e))

# sync_inspections.py [silent tag]
