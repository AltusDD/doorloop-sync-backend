# normalizers/normalized_properties.py

import datetime
from doorloop_client import fetch_properties

def normalize_properties() -> list[dict]:
    raw_properties = fetch_properties()
    normalized = []

    for raw in raw_properties:
        normalized.append({
            "doorloop_id": raw.get("id"),
            "name": raw.get("name"),
            "address_street1": raw.get("address", {}).get("street1"),
            "address_city": raw.get("address", {}).get("city"),
            "address_state": raw.get("address", {}).get("state"),
            "zip": raw.get("address", {}).get("zip"),
            "property_type": raw.get("type"),
            "class": raw.get("class"),
            "status": raw.get("status"),
            "total_sq_ft": raw.get("totalSqFt"),
            "unit_count": raw.get("unitCount"),
            "created_at": datetime.datetime.utcnow().isoformat(),
            "updated_at": datetime.datetime.utcnow().isoformat()
        })

    return normalized