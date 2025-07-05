
import os
import logging
from doorloop_client import DoorLoopClient
from supabase_client import SupabaseClient

API_SCHEMAS = {
    "accounts": ["id", "name", "status"],
    "properties": ["id", "name", "type", "status", "address", "city", "state", "zip"],
}

def clean_record(record):
    clean = {}
    for k, v in record.items():
        if isinstance(v, (str, int, float, bool)) or v is None:
            clean[k] = v
        elif isinstance(v, dict) and 'iso' in v:
            clean[k] = v['iso']
        else:
            continue
    return clean

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    doorloop_api_key = os.getenv("DOORLOOP_API_KEY")
    doorloop_base_url = os.getenv("DOORLOOP_API_BASE_URL")
    supabase_url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    dl_client = DoorLoopClient(api_key=doorloop_api_key, base_url=doorloop_base_url)
    sb_client = SupabaseClient(supabase_url=supabase_url, service_role_key=service_role_key)

    for endpoint, fields in API_SCHEMAS.items():
        logging.info(f"🔄 Syncing {endpoint}...")
        records = dl_client.fetch_all(endpoint)
        clean_records = [clean_record(r) for r in records]
        sb_client.upsert_data(endpoint, clean_records)
