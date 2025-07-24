import os
import json
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def normalize(table_name: str, target_table: str, fields: list):
    print(f"📥 Fetching data from {table_name}...")
    response = supabase.table(table_name).select("*").execute()
    records = response.data or []
    print(f"📊 Normalizing {len(records)} records from {table_name} → {target_table}")

    # Extract and normalize
    normalized = []
    for record in records:
        jsonb_data = record.get("data") or {}
        if not isinstance(jsonb_data, dict):
            try:
                jsonb_data = json.loads(jsonb_data)
            except Exception:
                continue
        norm = {}
        missing_fields = []

        # Always extract 'id' → 'doorloop_id'
        raw_id = jsonb_data.get("id")
        if raw_id:
            norm["doorloop_id"] = raw_id
        else:
            missing_fields.append("doorloop_id")

        for f in fields:
            norm[f] = jsonb_data.get(f)

        if not missing_fields:
            normalized.append(norm)

    print(f"🧹 Deleting all data from {target_table}...")
    supabase.table(target_table).delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()

    print(f"📤 Inserting {len(normalized)} normalized records into {target_table}")
    if normalized:
        for chunk in [normalized[i:i+50] for i in range(0, len(normalized), 50)]:
            supabase.table(target_table).insert(chunk).execute()
    else:
        print(f"⚠️ Dropped {len(records)} records due to missing required fields: ['doorloop_id']")

def run_normalization():
    normalize("doorloop_raw_properties", "doorloop_normalized_properties",
              ["name", "type", "class", "addressStreet1", "addressCity", "addressState"])
    normalize("doorloop_raw_units", "doorloop_normalized_units",
              ["name", "propertyId", "bedrooms", "bathrooms", "sqft", "marketRent"])
    normalize("doorloop_raw_leases", "doorloop_normalized_leases",
              ["unitId", "status", "startDate", "endDate"])
    normalize("doorloop_raw_tenants", "doorloop_normalized_tenants",
              ["firstName", "lastName", "email", "phone", "leaseId"])
    normalize("doorloop_raw_owners", "doorloop_normalized_owners",
              ["firstName", "lastName", "email", "phone"])

if __name__ == "__main__":
    print("🚀 Starting normalization process...")
    run_normalization()
