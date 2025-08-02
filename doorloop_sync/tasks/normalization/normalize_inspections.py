from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.audit.logger import log_audit_event

def run():
    entity = "normalize_inspections"
    log_audit_event(entity, "start")

    try:
        doorloop = DoorLoopClient()
        ingest = SupabaseIngestClient()
        records = doorloop.fetch_all("/" + entity.replace("normalize_", ""))
        ingest.upsert(entity.replace("normalize_", "doorloop_normalized_"), records)
        log_audit_event(entity, "success", {"normalized_count": len(records)})

    except Exception as e:
        log_audit_event(entity, "error", {"message": str(e)})
        print(f"❌ Error in {entity}:", e)
