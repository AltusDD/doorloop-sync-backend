from doorloop_sync.config import get_supabase_client
from doorloop_sync.audit.logger import log_audit_event

def normalize(record):
    return {
        "doorloop_id": record.get("id"),
        "name": record.get("name"),
        "address_street1": record.get("address", {}).get("street1"),
        "address_city": record.get("address", {}).get("city"),
        "address_state": record.get("address", {}).get("state"),
        "zip": record.get("address", {}).get("zip"),
        "property_type": record.get("type"),
        "class": record.get("class"),
        "status": record.get("status"),
        "total_sq_ft": record.get("squareFeet"),
        "unit_count": record.get("unitCount"),
        "occupied_units": record.get("occupiedUnits"),
        "occupancy_rate": record.get("occupancyRate"),
        "owner_id": record.get("ownerId"),
        "created_at": record.get("createdAt"),
        "updated_at": record.get("updatedAt"),
    }

def run():
    sb = get_supabase_client()
    log_audit_event("normalize_properties", "start")

    try:
        raw = sb.fetch_raw("doorloop_raw_properties")
        normalized = [normalize(r) for r in raw]
        sb.upsert_records("doorloop_normalized_properties", normalized)

        log_audit_event("normalize_properties", "success", metadata={"normalized_count": len(normalized)})
    except Exception as e:
        log_audit_event("normalize_properties", "error", error=True, metadata={"message": str(e)})
        raise