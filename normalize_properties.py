from typing import List, Dict, Any

def fetch_raw_data(client) -> List[Dict[str, Any]]:
    return client.fetch_raw_data("doorloop_raw_properties")

def normalize_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    # TODO: Implement actual normalization logic for properties
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        # ... add more fields as needed ...
    }
