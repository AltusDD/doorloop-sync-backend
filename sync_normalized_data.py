
import uuid
import os
from supabase import create_client, Client

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def mongodb_id_to_uuid(mongo_id: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_OID, mongo_id))

def sync_table(raw_table, normalized_table, field_map):
    raw_data = supabase.table(raw_table).select("*").execute().data
    records_to_insert = []

    print(f"🔍 Processing {len(raw_data)} records from {raw_table} → {normalized_table}...")

    for index, record in enumerate(raw_data):
        try:
            transformed = {}
            for raw_field, normalized_field in field_map.items():
                if raw_field in record:
                    value = record[raw_field]
                    if normalized_field == 'id' and isinstance(value, str) and len(value) == 24:
                        transformed['doorloop_id'] = value
                        transformed['id'] = mongodb_id_to_uuid(value)
                    elif normalized_field.endswith('_id') and isinstance(value, str) and len(value) == 24:
                        transformed[normalized_field] = mongodb_id_to_uuid(value)
                    else:
                        transformed[normalized_field] = value
            records_to_insert.append(transformed)

        except Exception as e:
            print(f"🚨 ERROR converting record {index} in {raw_table}")
            print(f"🔎 Raw record: {record}")
            print(f"📛 Field: {raw_field}, Value: {value}")
            print(f"🧨 Exception: {e}")
            raise

    try:
        supabase.table(normalized_table).insert(records_to_insert, upsert=True).execute()
        print(f"✅ Sync complete: {normalized_table} ({len(records_to_insert)} records)")
    except Exception as e:
        print("❌ Final INSERT/UPSERT failure")
        print(f"🧨 Failed payload: {records_to_insert}")
        raise

# Example usage:
if __name__ == "__main__":
    field_map = {
        "id": "id",
        "name": "name",
        "type": "type",
        "property": "property_id",
        "unit": "unit_id",
        "lease": "lease_id"
    }
    raw_table = "doorloop_raw_properties"
    normalized_table = "doorloop_normalized_properties"
    sync_table(raw_table, normalized_table, field_map)
