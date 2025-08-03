import logging
from supabase_client import SupabaseClient
from doorloop_sync.clients.doorloop_client import DoorLoopClient
from utils.transform_helpers import hash_dict, clean_dict

def run():
    logging.info("🔄 Starting normalization for lease_payments")

    supabase = SupabaseClient()
    raw_table = "doorloop_raw_lease_payments"
    normalized_table = "doorloop_normalized_lease_payments"

    raw_records = supabase.get_raw_records(raw_table)
    if not raw_records:
        logging.info("📭 No raw data found for lease_payments. Task complete.")
        return

    # De-duplicate using DoorLoop ID
    unique_records = {}
    for record in raw_records:
        data = record.get("data")
        if not data:
            continue
        doorloop_id = data.get("id")
        if doorloop_id:
            unique_records[doorloop_id] = data

    normalized = []
    for record in unique_records.values():
        cleaned = clean_dict(record)
        cleaned["doorloop_id"] = record.get("id")
        cleaned["source_hash"] = hash_dict(record)
        normalized.append(cleaned)

    supabase.upsert_records(normalized_table, normalized)
    logging.info(f"✅ Normalized {len(normalized)} lease_payments records")
