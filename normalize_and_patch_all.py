
# normalize_and_patch_all.py - Bulletproof Sync Engine
import re
import uuid
import requests
from supabase import create_client, Client

# Connect to Supabase
SUPABASE_URL = "https://your-project.supabase.co"
SUPABASE_SERVICE_ROLE_KEY = "your-secret-key"
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Canonical schema definition
SCHEMA = {
    "id": {"source": "id", "type": "uuid"},
    "doorloop_id": {"source": "_id", "type": "text"},
    "property_type": {"source": "propertyType", "type": "text"},
    "unit_count": {"source": "unitCount", "type": "int"},
    "occupied_units": {"source": "occupiedUnits", "type": "int"},
    "occupancy_rate": {"source": "occupancyRate", "type": "float"},
    "total_sqft": {"source": "totalSqft", "type": "float"},
    "owner_id": {"source": "ownerId", "type": "uuid"},
}

# Convert camelCase to snake_case
def camel_to_snake(name):
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()

# Convert Mongo ObjectId to UUIDv5
NAMESPACE = uuid.UUID("12345678-1234-5678-1234-567812345678")
def mongodb_id_to_uuid(object_id: str) -> str:
    return str(uuid.uuid5(NAMESPACE, object_id))

def fetch_table_columns(table_name):
    result = supabase.table(table_name).select("*").limit(1).execute()
    if result.data:
        return result.data[0].keys()
    else:
        # Fallback to fetch columns via Supabase REST if empty
        return [col for col in SCHEMA]

def transform_property(raw):
    transformed = {"id": mongodb_id_to_uuid(raw["_id"]), "doorloop_id": raw["_id"]}
    for field, meta in SCHEMA.items():
        source_key = meta["source"]
        if source_key in raw:
            value = raw[source_key]
            if meta["type"] == "uuid":
                value = mongodb_id_to_uuid(value) if value else None
            transformed[field] = value
    return transformed

def normalize(source_table, target_table):
    print(f"📥 Fetching from {source_table}...")
    raw_data = supabase.table(source_table).select("*").execute().data
    print(f"📊 Normalizing {len(raw_data)} records...")

    transformed_data = [transform_property(record) for record in raw_data]
    from supabase_schema_manager import SupabaseSchemaManager
    manager = SupabaseSchemaManager(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
    manager.add_missing_columns(target_table, transformed_data)

    existing_columns = fetch_table_columns(target_table)

    safe_data = [{k: v for k, v in item.items() if k in existing_columns} for item in transformed_data]

    print(f"🧹 Clearing {target_table}...")
    supabase.table(target_table).delete().neq("id", "").execute()

    print(f"📤 Inserting into {target_table}...")
    for chunk in [safe_data[i:i + 50] for i in range(0, len(safe_data), 50)]:
        supabase.table(target_table).insert(chunk).execute()

if __name__ == "__main__":
    normalize("doorloop_raw_properties", "doorloop_normalized_properties")
