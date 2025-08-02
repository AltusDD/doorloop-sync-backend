from doorloop_sync.config import get_supabase_client
from doorloop_sync.audit.logger import log_audit_event

def run():
    entity = "normalize_tenants"
    log_audit_event(entity, "start", False)
    try:
        supabase = get_supabase_client()
        raw = supabase.fetch_records("doorloop_raw_tenants")
        normalized = []

        for r in raw:
            normalized.append({
                "doorloop_id": r.get("id"),
                "created_at": r.get("createdAt"),
                "updated_at": r.get("updatedAt")
                # TODO: Map additional fields
            })

        supabase.upsert_records("doorloop_normalized_tenants", normalized)
        log_audit_event(entity, "success", False, {"normalized_count": len(normalized)})
    except Exception as e:
        log_audit_event(entity, "error", True, {"message": str(e)})
        raise
