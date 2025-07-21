
import os
import logging
from supabase import create_client, Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

logging.basicConfig(level=logging.INFO, format='%(message)s')

def is_valid_uuid(value):
    try:
        from uuid import UUID
        UUID(value)
        return True
    except Exception:
        return False

def sync_table(raw_table, normalized_table, field_map):
    logging.info(f"🔄 Syncing {raw_table} → {normalized_table}")

    raw_data = supabase.table(raw_table).select("*").execute().data

    records_to_insert = []
    for record in raw_data:
        new_record = {}

        # Generate valid UUID and preserve DoorLoop ID
        original_id = str(record.get("id", ""))
        new_record["doorloop_id"] = original_id

        if is_valid_uuid(original_id):
            new_record["id"] = original_id
        else:
            new_record["id"] = str(uuid4())

        for raw_field, normalized_field in field_map.items():
            new_record[normalized_field] = record.get(raw_field)

        records_to_insert.append(new_record)

    if records_to_insert:
        supabase.table(normalized_table).insert(records_to_insert, upsert=True).execute()
        logging.info(f"✅ Synced {len(records_to_insert)} records to {normalized_table}")

# Example: sync doorloop_raw_properties → doorloop_normalized_properties
sync_table(
    raw_table="doorloop_raw_properties",
    normalized_table="doorloop_normalized_properties",
    field_map={
        "name": "name",
        "addressStreet1": "address_street_1",
        "addressCity": "address_city",
        "addressState": "address_state",
        "zip": "zip",
        "propertyType": "property_type",
        "class": "property_class",
        "totalSqFt": "total_sq_ft",
        "unitCount": "unit_count",
        "status": "status"
    }
)
