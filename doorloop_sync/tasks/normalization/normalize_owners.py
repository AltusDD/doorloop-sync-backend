from doorloop_sync.clients.supabase_client import SupabaseIngestClient
from doorloop_sync.audit.logger import log_audit_event

def run():
    entity = "normalize_owners"
    log_audit_event(entity, "start")

    try:
        client = SupabaseIngestClient()
        raw_data = client.fetch_raw("doorloop_raw_owners")

        normalized = []
        for row in raw_data:
            normalized.append({
                "doorloop_id": row.get("id"),
                "name": row.get("name"),
                "email": row.get("email"),
                "phone": row.get("phone"),
                "status": row.get("status"),
                "property_ids": row.get("propertyIds"),
                "created_at": row.get("dateCreated"),
                "updated_at": row.get("dateUpdated"),
            })

        client.upsert("doorloop_normalized_owners", normalized)
        log_audit_event(entity, "success", {"normalized_count": len(normalized)})

    except Exception as e:
        log_audit_event(entity, "error", {"message": str(e)})
