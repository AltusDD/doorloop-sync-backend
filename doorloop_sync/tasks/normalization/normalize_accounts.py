from supabase_client import SupabaseClient
from doorloop_sync.services.json_utils import flatten_json_keys
from doorloop_sync.clients.doorloop_client import DoorLoopClient
import logging
import os

def run():
    logging.info("🔄 Running normalization: accounts")

    supabase = SupabaseClient()
    raw_data = supabase.fetch_raw_data("doorloop_raw_accounts")

    if not raw_data:
        logging.info("📭 No raw data found for accounts. Task complete.")
        return

    try:
        deduped = {record['id']: record for record in raw_data if 'id' in record}.values()
        flattened = flatten_json_keys(list(deduped))
        supabase.upsert_data("doorloop_normalized_accounts", flattened)
        logging.info("✅ Normalized and upserted accounts")
    except Exception as e:
        logging.error(f"❌ Error normalizing accounts: {e}")