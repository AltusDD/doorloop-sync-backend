from typing import List, Dict, Any

def fetch_raw_data(client) -> List[Dict[str, Any]]:
    return client.fetch_raw_data("doorloop_raw_units")

def normalize_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    # TODO: Implement actual normalization logic for units
    return {
        "id": raw.get("id"),
        "property_id": raw.get("property_id"),
        # ... add more fields as needed ...
    }
