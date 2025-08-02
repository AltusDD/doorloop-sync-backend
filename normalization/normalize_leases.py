from typing import List, Dict, Any

from supabase import Client as SupabaseIngestClient  # Add this import if using supabase-py

def fetch_raw_data(client: SupabaseIngestClient) -> List[Dict[str, Any]]:
    return client.fetch_raw_data("doorloop_raw_leases")

def normalize_record(raw: Dict[str, Any]) -> Dict[str, Any]:
    # TODO: Implement actual normalization logic for leases
    return {
        "id": raw.get("id"),
        "unit_id": raw.get("unit_id"),
        # ... add more fields as needed ...
    }
