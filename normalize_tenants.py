from typing import List, Dict, Any
from supabase import SupabaseClient

def fetch_raw_data(client: SupabaseClient) -> List[Dict[str, Any]]:
    return client.fetch_raw_data("doorloop_raw_tenants")

def normalize_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    # TODO: Implement actual normalization logic for tenants
    return {
        "id": raw.get("id"),
        "lease_id": raw.get("lease_id"),
        # ... add more fields as needed ...
    }
