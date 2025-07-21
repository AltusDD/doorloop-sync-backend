import os
from supabase import create_client, Client
from dotenv import load_dotenv
import uuid

# Load environment variables
load_dotenv()
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def is_valid_uuid(val):
    try:
        uuid.UUID(str(val))
        return True
    except Exception:
        return False

def sync_table(raw_table, normalized_table, field_map):
    print(f"🔄 Syncing {raw_table} → {normalized_table}")
    raw_data = supabase.table(raw_table).select("*").execute().data

    records_to_insert = []
    for record in raw_data:
        normalized_record = {}
        skip = False
        for norm_field, raw_field in field_map.items():
            value = record.get(raw_field)
            if norm_field in ['id', 'property', 'unit', 'lease', 'tenant'] and value and not is_valid_uuid(value):
                print(f"⚠️ Skipping record due to invalid UUID in {norm_field}: {value}")
                skip = True
                break
            normalized_record[norm_field] = value
        if not skip:
            records_to_insert.append(normalized_record)

    if records_to_insert:
        supabase.table(normalized_table).insert(records_to_insert, upsert=True).execute()
        print(f"✅ Inserted {len(records_to_insert)} records into {normalized_table}")
    else:
        print(f"⚠️ No valid records to insert for {normalized_table}")

# Define mappings
mappings = {
    "doorloop_normalized_properties": {
        "id": "id",
        "name": "name",
        "address": "addressStreet1"
    },
    "doorloop_normalized_units": {
        "id": "id",
        "property": "propertyId",
        "unit_number": "name",
        "bedroom_count": "bedrooms"
    },
    "doorloop_normalized_owners": {
        "id": "id",
        "name": "name"
    },
    "doorloop_normalized_leases": {
        "id": "id",
        "property": "propertyId",
        "unit": "unitId",
        "monthly_rent": "rentAmount",
        "status": "status"
    },
    "doorloop_normalized_tenants": {
        "id": "id",
        "first_name": "firstName",
        "last_name": "lastName",
        "email": "email"
    },
    "doorloop_normalized_lease_tenants": {
        "lease": "leaseId",
        "tenant": "tenantId"
    },
    "doorloop_normalized_payments": {
        "id": "id",
        "lease": "leaseId",
        "amount": "amount",
        "payment_date": "date"
    }
}

# Run syncs
for normalized_table, field_map in mappings.items():
    raw_table = normalized_table.replace("normalized", "raw")
    sync_table(raw_table, normalized_table, field_map)
