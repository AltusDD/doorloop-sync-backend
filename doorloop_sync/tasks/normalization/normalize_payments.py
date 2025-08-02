from doorloop_sync.clients.supabase_client import SupabaseIngestClient
from doorloop_sync.audit.logger import log_audit_event

def run():
    entity = "normalize_payments"
    log_audit_event(entity, "start")

    try:
        client = SupabaseIngestClient()
        raw_data = client.fetch_raw("doorloop_raw_payments")

        normalized = []
        for row in raw_data:
            normalized.append({
                "doorloop_id": row.get("id"),
                "lease_id": row.get("leaseId"),
                "amount": row.get("amount"),
                "payment_method": row.get("paymentMethod"),
                "status": row.get("status"),
                "payment_date": row.get("paymentDate"),
                "created_at": row.get("dateCreated"),
                "updated_at": row.get("dateUpdated"),
            })

        client.upsert("doorloop_normalized_payments", normalized)
        log_audit_event(entity, "success", {"normalized_count": len(normalized)})

    except Exception as e:
        log_audit_event(entity, "error", {"message": str(e)})
