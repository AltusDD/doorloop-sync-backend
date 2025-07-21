import os
import uuid
from supabase import create_client, Client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def sync_table(raw_table: str, normalized_table: str, field_map: dict):
    print(f"🔄 Syncing {raw_table} → {normalized_table}")
    try:
        raw_records = supabase.table(raw_table).select("*").execute().data
    except Exception as e:
        print(f"❌ Failed to fetch from {raw_table}: {e}")
        return

    records_to_insert = []
    for record in raw_records:
        new_record = {
            db_field: record.get(raw_field)
            for raw_field, db_field in field_map.items()
        }
        # Inject UUID for normalized id
        new_record['id'] = str(uuid.uuid4())

        # Store DoorLoop original ID separately if present
        if 'id' in record:
            new_record['doorloop_id'] = record['id']
        records_to_insert.append(new_record)

    try:
        supabase.table(normalized_table).insert(records_to_insert, upsert=True).execute()
        print(f"✅ Inserted {len(records_to_insert)} records into {normalized_table}")
    except Exception as e:
        print(f"🔥 Insert failed for {normalized_table}: {e}")

# Field mappings (example for properties, expand as needed)
field_mappings = {
    'doorloop_raw_properties': {
        'name': 'name',
        'addressStreet1': 'address_street_1',
        'addressCity': 'address_city',
        'addressState': 'address_state',
        'zip': 'zip_code',
        'propertyType': 'property_type',
        'status': 'status'
    }
}

if __name__ == "__main__":
    for raw_table, field_map in field_mappings.items():
        normalized_table = raw_table.replace("raw", "normalized")
        sync_table(raw_table, normalized_table, field_map)
