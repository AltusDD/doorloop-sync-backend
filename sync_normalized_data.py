import os
import uuid
import logging
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise EnvironmentError("Missing Supabase credentials in environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
logging.basicConfig(level=logging.INFO)

def generate_uuid():
    return str(uuid.uuid4())

def sync_table(raw_table, normalized_table, field_map):
    logging.info(f"🔄 Syncing {raw_table} → {normalized_table}")
    response = supabase.table(raw_table).select("*").execute()
    if not response.data:
        logging.warning(f"⚠️ No data found in {raw_table}")
        return

    records_to_insert = []
    for raw_record in response.data:
        new_record = {}
        for norm_field, raw_field in field_map.items():
            new_record[norm_field] = raw_record.get(raw_field)

        # Use the raw DoorLoop ID as doorloop_id (TEXT), and generate a true UUID for internal ID
        new_record["doorloop_id"] = raw_record.get("id")
        new_record["id"] = generate_uuid()
        records_to_insert.append(new_record)

    supabase.table(normalized_table).insert(records_to_insert, upsert=True).execute()
    logging.info(f"✅ Synced {len(records_to_insert)} records to {normalized_table}")

if __name__ == "__main__":
    sync_table(
        raw_table="doorloop_raw_properties",
        normalized_table="doorloop_normalized_properties",
        field_map={
            "name": "name",
            "addressStreet1": "address",
            "addressCity": "city",
            "addressState": "state",
            "zip": "zip"
        }
    )
