from doorloop_sync.clients.supabase_client import SupabaseIngestClient
from doorloop_sync.audit.logger import log_audit_event

def run():
    entity = "normalize_leases"
    log_audit_event(entity, "start")

    try:
        client = SupabaseIngestClient()
        raw_data = client.fetch_raw("doorloop_raw_leases")

        normalized = []
        for row in raw_data:
            normalized.append({
                "doorloop_id": row.get("id"),
                "property_id": row.get("propertyId"),
                "unit_id": row.get("unitId"),
                "tenant_ids": row.get("tenantIds"),
                "status": row.get("status"),
                "start_date": row.get("startDate"),
                "end_date": row.get("endDate"),
                "rent_amount": row.get("rentAmount"),
                "deposit_amount": row.get("depositAmount"),
                "created_at": row.get("dateCreated"),
                "updated_at": row.get("dateUpdated"),
            })

        client.upsert("doorloop_normalized_leases", normalized)
        log_audit_event(entity, "success", {"normalized_count": len(normalized)})

    except Exception as e:
        log_audit_event(entity, "error", {"message": str(e)})
