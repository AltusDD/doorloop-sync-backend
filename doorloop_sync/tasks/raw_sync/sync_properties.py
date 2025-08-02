from doorloop_sync.config import get_doorloop_client, get_supabase_client
from doorloop_sync.audit.logger import log_audit_event

def run():
    dl = get_doorloop_client()
    sb = get_supabase_client()

    log_audit_event("sync_properties", "start")

    try:
        records = dl.get("/properties")
        log_audit_event("sync_properties", "data_received", metadata={"count": len(records)})

        sb.upsert_records("doorloop_raw_properties", records)
        log_audit_event("sync_properties", "upserted", metadata={"count": len(records)})

    except Exception as e:
        log_audit_event("sync_properties", "error", error=True, metadata={"message": str(e)})
        raise