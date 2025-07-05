
import os
import logging
from doorloop_client import DoorLoopClient
from supabase_client import SupabaseClient
from supabase_schema_manager import SupabaseSchemaManager

logging.basicConfig(level=logging.DEBUG)

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
    logging.info("🚀 Starting full DoorLoop → Supabase sync")

    doorloop_api_key = os.getenv("DOORLOOP_API_KEY")
    doorloop_base_url = os.getenv("DOORLOOP_API_BASE_URL")
    supabase_url = os.getenv("SUPABASE_URL")
    service_role_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

    logging.debug("🔐 Environment variables loaded")

    dl_client = DoorLoopClient(api_key=doorloop_api_key, base_url=doorloop_base_url)
    sb_client = SupabaseClient(url=supabase_url, service_role_key=service_role_key)
    schema_manager = SupabaseSchemaManager(supabase_url, service_role_key)

    endpoints = ["accounts"]

    for endpoint in endpoints:
        logging.info(f"🔄 Syncing {endpoint}...")
        records = dl_client.fetch_all(endpoint)
        if not records:
            logging.warning(f"⚠️ No records returned for {endpoint}")
            continue
        clean_records = [clean_record(r) for r in records]
        logging.debug(f"🧼 Cleaned {len(clean_records)} records for {endpoint}")
        schema_manager.add_missing_columns(endpoint, clean_records)
        sb_client.upsert_data(endpoint, clean_records)
