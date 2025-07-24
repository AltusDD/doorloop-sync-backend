
import os
import json
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def normalize(source_table, target_table, transform_function):
    print(f"📥 Fetching data from {source_table}...")
    raw_data = supabase.table(source_table).select("*").execute().data
    print(f"📊 Normalizing {len(raw_data)} records from {source_table} → {target_table}")

    normalized_data = []
    for row in raw_data:
        normalized_row = transform_function(row)
        if normalized_row:
            normalized_data.append(normalized_row)

    print(f"🧹 Deleting all data from {target_table}...")
    supabase.table(target_table).delete().neq("id", "0").execute()
    print(f"📤 Inserting {len(normalized_data)} normalized records into {target_table}")
    for i in range(0, len(normalized_data), 100):
        chunk = normalized_data[i:i + 100]
        supabase.table(target_table).insert(chunk).execute()

def transform_property(row):
    data = row.get("data", {})
    if not data.get("id"):
        print(f"⚠️ Skipped row missing id: {row}")
        return None

    return {
        "doorloop_id": data.get("id"),
        "name": data.get("name"),
        "addressStreet1": data.get("address", {}).get("street1"),
        "addressCity": data.get("address", {}).get("city"),
        "addressState": data.get("address", {}).get("state"),
        "zip": data.get("address", {}).get("zip"),
        "propertyType": data.get("type"),
        "class": data.get("class"),
        "status": "active" if data.get("active") else "inactive",
        "unitCount": data.get("numActiveUnits", 0),
        "created_at": data.get("createdAt"),
        "updated_at": data.get("updatedAt"),
    }

def run_normalization():
    normalize("doorloop_raw_properties", "doorloop_normalized_properties", transform_property)

if __name__ == "__main__":
    print("🚀 Starting normalization process...")
    run_normalization()
