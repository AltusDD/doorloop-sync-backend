import os
import json
from supabase import create_client, Client
from utils import extract_id_from_jsonb

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def normalize(table, target_table, id_key):
    print(f"📥 Fetching data from {table}...")
    raw_data = supabase.table(table).select("*").execute().data
    print(f"📊 Normalizing {len(raw_data)} records from {table} → {target_table}")

    normalized_data = []
    for row in raw_data:
        normalized = {}
        raw_json = row.get("raw", {})
        doorloop_id = extract_id_from_jsonb(raw_json)
        if doorloop_id:
            normalized["doorloop_id"] = doorloop_id
            normalized.update(raw_json)
            normalized_data.append(normalized)
        else:
            print(f"⚠️ Skipped row missing {id_key}: {row}")

    print(f"🧹 Deleting all data from {target_table}...")
    supabase.table(target_table).delete().neq("id", "").execute()
    print(f"📤 Inserting {len(normalized_data)} normalized records into {target_table}")
    if normalized_data:
        supabase.table(target_table).insert(normalized_data).execute()

def run_normalization():
    normalize("doorloop_raw_properties", "doorloop_normalized_properties", "id")
    normalize("doorloop_raw_units", "doorloop_normalized_units", "id")
    normalize("doorloop_raw_leases", "doorloop_normalized_leases", "id")
    normalize("doorloop_raw_tenants", "doorloop_normalized_tenants", "id")
    normalize("doorloop_raw_owners", "doorloop_normalized_owners", "id")

if __name__ == "__main__":
    print("🚀 Starting normalization process...")
    run_normalization()
