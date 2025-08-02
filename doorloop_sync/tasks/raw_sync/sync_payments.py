from doorloop_sync.config import get_doorloop_client, get_supabase_client
from doorloop_sync.audit.logger import log_audit_event

def run():
    entity = "sync_payments"
    log_audit_event(entity, "start", False)
    try:
        client = get_doorloop_client()
        data = client.get("payments")  # Adjust endpoint if needed
        supabase = get_supabase_client()
        supabase.upsert_records("doorloop_raw_payments", data)
        log_audit_event(entity, "success", False, {"count": len(data)})
    except Exception as e:
        log_audit_event(entity, "error", True, {"message": str(e)})
        raise
