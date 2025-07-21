import uuid
from supabase import create_client
import os

url = os.environ.get("SUPABASE_URL")
key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(url, key)

def is_valid_uuid(val: str) -> bool:
    try:
        uuid.UUID(str(val))
        return True
    except:
        return False

def sync_table(raw_table, normalized_table, field_map):
    print(f"🔄 Syncing {raw_table} → {normalized_table}")

    raw_data = supabase.table(raw_table).select("*").execute().data
    records_to_insert = []

    for record in raw_data:
        new_record = {}

        # Use a valid UUID for id or generate one
        new_record["id"] = str(uuid.uuid4())

        # Always store the DoorLoop _id as doorloop_id
        new_record["doorloop_id"] = record.get("_id", None)

        for raw_field, normalized_field in field_map.items():
            new_record[normalized_field] = record.get(raw_field)

        records_to_insert.append(new_record)

    if records_to_insert:
        supabase.table(normalized_table).insert(records_to_insert, upsert=True).execute()
        print(f"✅ Synced {len(records_to_insert)} records to {normalized_table}")
    else:
        print("⚠️ No records to sync.")
