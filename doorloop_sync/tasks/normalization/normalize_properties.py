from doorloop_sync.clients.supabase_client import SupabaseIngestClient
from doorloop_sync.audit.logger import log_audit_event

def run():
    entity = "normalize_properties"
    log_audit_event(entity, "start")

    try:
        client = SupabaseIngestClient()
        raw_data = client.fetch_raw("doorloop_raw_properties")

        normalized = []
        for row in raw_data:
            normalized.append({
                "doorloop_id": row.get("id"),
                "name": row.get("name"),
                "address_street1": row.get("address", {}).get("street1"),
                "address_city": row.get("address", {}).get("city"),
                "address_state": row.get("address", {}).get("state"),
                "zip": row.get("address", {}).get("zip"),
                "property_type": row.get("type"),
                "class": row.get("class"),
                "status": row.get("status"),
                "total_sq_ft": row.get("squareFeet"),
                "unit_count": row.get("unitCount"),
                "occupied_units": row.get("occupiedUnits"),
                "occupancy_rate": row.get("occupancyRate"),
                "owner_id": row.get("ownerId"),
                "created_at": row.get("dateCreated"),
                "updated_at": row.get("dateUpdated"),
            })

        client.upsert("doorloop_normalized_properties", normalized)
        log_audit_event(entity, "success", {"normalized_count": len(normalized)})

    except Exception as e:
        log_audit_event(entity, "error", {"message": str(e)})
