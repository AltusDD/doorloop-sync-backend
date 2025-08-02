from doorloop_sync.clients.supabase_client import SupabaseIngestClient
from doorloop_sync.audit.logger import log_audit_event

def run():
    entity = "normalize_units"
    log_audit_event(entity, "start")

    try:
        client = SupabaseIngestClient()
        raw_data = client.fetch_raw("doorloop_raw_units")

        normalized = []
        for row in raw_data:
            normalized.append({
                "doorloop_id": row.get("id"),
                "name": row.get("name"),
                "property_id": row.get("propertyId"),
                "status": row.get("status"),
                "bedrooms": row.get("bedrooms"),
                "bathrooms": row.get("bathrooms"),
                "square_feet": row.get("squareFeet"),
                "market_rent": row.get("marketRent"),
                "created_at": row.get("dateCreated"),
                "updated_at": row.get("dateUpdated"),
            })

        client.upsert("doorloop_normalized_units", normalized)
        log_audit_event(entity, "success", {"normalized_count": len(normalized)})

    except Exception as e:
        log_audit_event(entity, "error", {"message": str(e)})
