from doorloop_sync.config import get_supabase_client
from doorloop_sync.audit.logger import log_audit_event

def normalize(record):
    return {
        "doorloop_id": record.get("id"),
        "property_id": record.get("propertyId"),
        "name": record.get("name"),
        "bedrooms": record.get("bedrooms"),
        "bathrooms": record.get("bathrooms"),
        "square_feet": record.get("squareFeet"),
        "market_rent": record.get("marketRent"),
        "status": record.get("status"),
        "created_at": record.get("createdAt"),
        "updated_at": record.get("updatedAt"),
    }

def run():
    sb = get_supabase_client()
    log_audit_event("normalize_units", "start")

    try:
        raw = sb.fetch_raw("doorloop_raw_units")
        normalized = [normalize(r) for r in raw]
        sb.upsert_records("doorloop_normalized_units", normalized)

        log_audit_event("normalize_units", "success", metadata={"normalized_count": len(normalized)})
    except Exception as e:
        log_audit_event("normalize_units", "error", error=True, metadata={"message": str(e)})
        raise