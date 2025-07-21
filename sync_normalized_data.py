
import os
import uuid
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Example mapping for one table - expand as needed
table_mappings = [
    {
        "raw_table": "doorloop_raw_properties",
        "normalized_table": "doorloop_normalized_properties",
        "field_map": {
            "name": "name",
            "class": "class",
            "typeDescription": "type_description",
            "numActiveUnits": "unit_count"
        }
    }
]

def is_valid_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except ValueError:
        return False

def sync_table(raw_table, normalized_table, field_map):
    print(f"🔄 Syncing {raw_table} → {normalized_table}")
    raw_data = supabase.table(raw_table).select("*").execute().data

    records_to_insert = []
    for row in raw_data:
        data = row.get("data", {})
        new_record = {}

        # Move original DoorLoop ID into doorloop_id
        new_record["doorloop_id"] = data.get("id", None)

        # Create new UUID for id field
        new_record["id"] = str(uuid.uuid4())

        # Map fields based on schema
        for raw_field, normalized_field in field_map.items():
            new_record[normalized_field] = data.get(raw_field)

        records_to_insert.append(new_record)

    if records_to_insert:
        supabase.table(normalized_table).insert(records_to_insert, upsert=True).execute()
        print(f"✅ Inserted {len(records_to_insert)} rows into {normalized_table}")
    else:
        print("⚠️ No records found to insert.")

if __name__ == "__main__":
    for mapping in table_mappings:
        sync_table(mapping["raw_table"], mapping["normalized_table"], mapping["field_map"])
