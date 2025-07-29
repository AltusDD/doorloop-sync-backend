from typing import List, Dict, Any

from supabase import Client as SupabaseClient  # Add this import if using supabase-py

def fetch_raw_data(client: SupabaseClient) -> List[Dict[str, Any]]:
    return client.fetch_raw_data("doorloop_raw_owners")

def normalize_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    # TODO: Implement actual normalization logic for owners
    return {
        "id": raw.get("id"),
        "name": raw.get("name"),
        # ... add more fields as needed ...
    }
