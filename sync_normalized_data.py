# force trigger
import os
import uuid
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def mongodb_id_to_uuid(mongo_id: str) -> str:
    """Converts a MongoDB-style ObjectId to a UUIDv5 deterministically."""
    return str(uuid.uuid5(uuid.NAMESPACE_OID, mongo_id))

def sync_table(raw_table, normalized_table, field_map):
    print(f"🔄 Syncing {raw_table} → {normalized_table}")
    response = supabase.table(raw_table).select("*").execute()
    raw_data = response.data

    records_to_insert = []
    for record in raw_data:
        transformed = {}
        for raw_field, normalized_field in field_map.items():
            if raw_field in record:
                value = record[raw_field]
                # Convert known Mongo ObjectIds to UUID
                if normalized_field == 'id' and isinstance(value, str) and len(value) == 24:
                    transformed['doorloop_id'] = value
                    transformed['id'] = mongodb_id_to_uuid(value)
                elif normalized_field.endswith('_id') and isinstance(value, str) and len(value) == 24:
                    transformed[normalized_field] = mongodb_id_to_uuid(value)
                else:
                    transformed[normalized_field] = value
        records_to_insert.append(transformed)

    if records_to_insert:
        supabase.table(normalized_table).insert(records_to_insert, upsert=True).execute()

# Define mappings
field_map = {
    "id": "id",
    "name": "name",
    "type": "type",
    "address": "address",
    "city": "city",
    "state": "state",
    "zip": "zip",
    "owner": "owner_id",
    "manager": "manager_id",
}

sync_table("doorloop_raw_properties", "doorloop_normalized_properties", field_map)
