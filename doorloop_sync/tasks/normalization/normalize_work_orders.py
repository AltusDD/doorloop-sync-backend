from doorloop_sync.clients.supabase_client import SupabaseIngestClient
from doorloop_sync.audit.logger import log_audit_event

def run():
    entity = "normalize_work_orders"
    log_audit_event(entity, "start")

    try:
        client = SupabaseIngestClient()
        raw_data = client.fetch_raw("doorloop_raw_work_orders")

        normalized = []
        for row in raw_data:
            normalized.append({
                "doorloop_id": row.get("id"),
                "property_id": row.get("propertyId"),
                "unit_id": row.get("unitId"),
                "vendor_id": row.get("vendorId"),
                "status": row.get("status"),
                "priority": row.get("priority"),
                "scheduled_date": row.get("scheduledDate"),
                "completed_date": row.get("completedDate"),
                "description": row.get("description"),
                "created_at": row.get("dateCreated"),
                "updated_at": row.get("dateUpdated"),
            })

        client.upsert("doorloop_normalized_work_orders", normalized)
        log_audit_event(entity, "success", {"normalized_count": len(normalized)})

    except Exception as e:
        log_audit_event(entity, "error", {"message": str(e)})
