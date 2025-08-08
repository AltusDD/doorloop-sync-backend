from doorloop_sync.clients.doorloop_client import DoorLoopClient
from doorloop_sync.clients.supabase_ingest_client import SupabaseIngestClient
from doorloop_sync.utils.logger import log_sync_start, log_sync_end, log_error
from doorloop_sync.utils.utils_data import standardize_record

def sync_properties():
    """
    Fetches core property data from DoorLoop and upserts it into the 'properties' table.
    """
    entity = "properties"
    log_sync_start(entity)
    
    try:
        client = DoorLoopClient()
        supabase = SupabaseIngestClient()
        
        all_records = client.get_all("/api/properties")
        
        if not all_records:
            log_sync_end(entity, 0)
            return

        normalized_records = []
        for item in all_records:
            address = item.get("address", {}) or {}
            record = {
                "doorloop_id": item.get("id"),
                "name": item.get("name"),
                "type": item.get("type"),
                "property_class": item.get("class"),
                "active": item.get("active", True),
                "description": item.get("description"),
                "external_id": item.get("externalId"),
                "manager_id": item.get("managerId"),
                "purchase_date": item.get("purchaseDate"),
                "purchase_price_cents": int(float(item.get("purchasePrice", 0)) * 100),
                "current_value_cents": int(float(item.get("currentValue", 0)) * 100),
                "bedroom_count": item.get("bedroomCount"),
                "address_street1": address.get("street1"),
                "address_street2": address.get("street2"),
                "address_city": address.get("city"),
                "address_state": address.get("state"),
                "address_zip": address.get("zip"),
                "address_country": address.get("country"),
                "address_lat": address.get("lat"),
                "address_lng": address.get("lng"),
                "address_is_valid": address.get("isValidAddress"),
                "created_at": item.get("createdAt"),
                "updated_at": item.get("updatedAt"),
            }
            normalized_records.append(standardize_record(record))

        supabase.insert_records("properties", normalized_records, entity)
        log_sync_end(entity, len(normalized_records))

    except Exception as e:
        log_error(entity, str(e))
