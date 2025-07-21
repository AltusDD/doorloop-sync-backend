import os
import json
from datetime import datetime
from supabase import create_client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

NORMALIZATION_TARGETS = {
    "properties": {
        "raw_table": "doorloop_raw_properties",
        "normalized_table": "doorloop_normalized_properties",
        "fields": [
            "id", "doorloop_id", "name", "type", "class", "description",
            "address_street1", "address_city", "address_state", "zip",
            "active", "total_sqft", "unit_count", "occupied_units",
            "occupancy_rate", "created_at", "updated_at"
        ],
    },
    "units": {
        "raw_table": "doorloop_raw_units",
        "normalized_table": "doorloop_normalized_units",
        "fields": [
            "id", "doorloop_id", "property_id", "name", "status",
            "bedrooms", "bathrooms", "sqft", "market_rent", "active",
            "created_at", "updated_at"
        ],
    },
    "leases": {
        "raw_table": "doorloop_raw_leases",
        "normalized_table": "doorloop_normalized_leases",
        "fields": [
            "id", "doorloop_id", "unit_id", "tenant_id",
            "start_date", "end_date", "status", "rent_amount",
            "batch", "created_at", "updated_at"
        ],
    },
    "tenants": {
        "raw_table": "doorloop_raw_tenants",
        "normalized_table": "doorloop_normalized_tenants",
        "fields": [
            "id", "doorloop_id", "full_name", "primary_email",
            "primary_phone", "status", "accepted_on_tos",
            "created_at", "updated_at"
        ],
    },
    "owners": {
        "raw_table": "doorloop_raw_owners",
        "normalized_table": "doorloop_normalized_owners",
        "fields": [
            "id", "doorloop_id", "full_name", "primary_email",
            "primary_phone", "active", "created_at", "updated_at"
        ],
    },
}

def normalize_record(record, fields):
    normalized = {}
    for field in fields:
        val = record.get(field)
        if isinstance(val, (dict, list)):
            normalized[field] = json.dumps(val)
        else:
            normalized[field] = val
    return normalized

def run_normalization():
    for label, config in NORMALIZATION_TARGETS.items():
        raw_table = config["raw_table"]
        norm_table = config["normalized_table"]
        fields = config["fields"]

        print(f"📥 Fetching data from {raw_table}...")
        response = supabase.table(raw_table).select("*").execute()
        rows = response.data
        print(f"📊 Normalizing {len(rows)} records from {raw_table} → {norm_table}")

        print(f"🧹 Deleting all data from {norm_table}...")
        try:
            supabase.table(norm_table).delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()
        except Exception as e:
            print(f"⚠️ DELETE failed on {norm_table}: {e}")

        to_insert = [normalize_record(r, fields) for r in rows]

        if not to_insert:
            print(f"⛔ No data to insert into {norm_table}")
            continue

        print(f"📤 Inserting {len(to_insert)} normalized records into {norm_table}")
        try:
            supabase.table(norm_table).insert(to_insert).execute()
        except Exception as e:
            print(f"❌ Failed to normalize table {label}: {e}")

if __name__ == "__main__":
    print(f"🚀 Starting normalization process...")
    run_normalization()
