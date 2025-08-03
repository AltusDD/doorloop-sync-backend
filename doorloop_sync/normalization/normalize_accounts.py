from supabase_client import SupabaseClient
from doorloop_client import DoorLoopClient
from doorloop_sync.utils.transform_helpers import hash_dict, clean_dict

def run():
    print("🔄 Normalizing accounts...")
    raw_data = SupabaseClient().get_raw("accounts")
    seen_ids = set()
    normalized = []

    for record in raw_data:
        record_id = record.get("id")
        if record_id and record_id not in seen_ids:
            seen_ids.add(record_id)
            cleaned = clean_dict(record)
            normalized.append(cleaned)

    SupabaseClient().upsert_normalized("accounts", normalized)
    print(f"✅ Normalized {len(normalized)} accounts")