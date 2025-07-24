import os
import json
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def normalize_records(raw_records, required_fields):
    """ Filter and prepare normalized records, ensuring required fields are present """
    normalized = []
    dropped = []
    for record in raw_records:
        if all(record.get(field) for field in required_fields):
            normalized.append(record)
        else:
            dropped.append(record)
    return normalized, dropped

def process_table(raw_table, norm_table, required_fields):
    print(f"📥 Fetching data from {raw_table}...")
    raw_data = supabase.table(raw_table).select("*").execute().data
    print(f"📊 Normalizing {len(raw_data)} records from {raw_table} → {norm_table}")

    normalized, dropped = normalize_records(raw_data, required_fields)

    print(f"🧹 Deleting all data from {norm_table}...")
    supabase.table(norm_table).delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()

    print(f"📤 Inserting {len(normalized)} normalized records into {norm_table}")
    if normalized:
        response = supabase.table(norm_table).insert(normalized).execute()
        if hasattr(response, 'data'):
            print(f"✅ Inserted {len(response.data)} records into {norm_table}")
        else:
            print(f"⚠️ Unexpected response: {response}")
    if dropped:
        print(f"⚠️ Dropped {len(dropped)} records due to missing required fields: {required_fields}")

def run_normalization():
    try:
        process_table("doorloop_raw_properties", "doorloop_normalized_properties", ["doorloop_id"])
        process_table("doorloop_raw_units", "doorloop_normalized_units", ["doorloop_id"])
        process_table("doorloop_raw_leases", "doorloop_normalized_leases", ["doorloop_id"])
        process_table("doorloop_raw_tenants", "doorloop_normalized_tenants", ["doorloop_id"])
        process_table("doorloop_raw_owners", "doorloop_normalized_owners", ["doorloop_id"])
    except Exception as e:
        print(f"❌ ERROR during normalization: {e}")

if __name__ == "__main__":
    print("🚀 Starting normalization process...")
    run_normalization()
