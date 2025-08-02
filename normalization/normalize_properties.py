import logging

logger = logging.getLogger(__name__)

def run(raw_properties: list[dict]) -> list[dict]:
    """Normalize raw DoorLoop property records into a consistent schema."""
    logger.info("🧮 Normalizing raw property records...")

    normalized = []
    for item in raw_properties:
        normalized.append({
            "doorloop_id": item.get("id"),
            "name": item.get("name"),
            "address_street1": item.get("address", {}).get("street1"),
            "address_city": item.get("address", {}).get("city"),
            "address_state": item.get("address", {}).get("state"),
            "zip": item.get("address", {}).get("zip"),
            "property_type": item.get("type"),
            "class": item.get("class"),
            "status": item.get("status"),
            "total_sq_ft": item.get("totalSquareFeet"),
            "unit_count": item.get("unitCount"),
            "occupied_units": item.get("occupiedUnits"),
            "occupancy_rate": item.get("occupancyRate"),
            "created_at": item.get("createdAt"),
            "updated_at": item.get("updatedAt"),
        })

    logger.info(f"✅ Normalized {len(normalized)} properties.")
    return normalized
