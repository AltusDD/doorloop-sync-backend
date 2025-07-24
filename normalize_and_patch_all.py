import os
import json
import uuid
import logging
from supabase import create_client, Client
from dotenv import load_dotenv
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logging.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def camel_to_snake(name: str) -> str:
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

def mongodb_id_to_uuid(mongo_id: str) -> str:
    if not isinstance(mongo_id, str) or len(mongo_id) != 24:
        return None
    return str(uuid.uuid5(uuid.NAMESPACE_OID, mongo_id))

TARGET_SCHEMA = {
    'doorloop_normalized_properties': {
        'id': 'uuid',
        'doorloop_id': 'text',
        'name': 'text',
        'property_type': 'text',
        'address_street1': 'text',
        'address_city': 'text',
        'address_state': 'text',
        'address_zip': 'text',
        'manager_id': 'uuid',
        'class': 'text',
        'status': 'text',
        'unit_count': 'integer',
        'created_at': 'timestamp with time zone',
        'updated_at': 'timestamp with time zone',
        'pictures_json': 'jsonb',
    }
}

def transform_property(row: dict) -> dict:
    data = row.get("data", {})
    if not data.get("id"):
        logging.warning(f"⚠️ Skipped row missing id: {row}")
        return None

    transformed_data = {camel_to_snake(k): v for k, v in data.items()}
    transformed_data["id"] = mongodb_id_to_uuid(transformed_data.get("id"))
    transformed_data["doorloop_id"] = transformed_data.get("id")

    if transformed_data.get("manager_id"):
        transformed_data["manager_id"] = mongodb_id_to_uuid(transformed_data.get("manager_id"))

    if 'num_active_units' in transformed_data:
        transformed_data['unit_count'] = transformed_data.pop('num_active_units')
    if 'type' in transformed_data:
        transformed_data['property_type'] = transformed_data.pop('type')

    return {k: v for k, v in transformed_data.items() if k in TARGET_SCHEMA['doorloop_normalized_properties']}

def normalize(source_table: str, target_table: str, transform_function: callable):
    logging.info(f"📥 Fetching data from {source_table}...")
    try:
        raw_data = supabase.table(source_table).select("*").execute().data
        logging.info(f"📊 Normalizing {len(raw_data)} records from {source_table} → {target_table}")
    except Exception as e:
        logging.error(f"❌ Failed to fetch data from {source_table}: {e}")
        raise

    normalized_data = []
    for row in raw_data:
        normalized_row = transform_function(row)
        if normalized_row:
            normalized_data.append(normalized_row)

    try:
        logging.info(f"🧹 Deleting all data from {target_table}...")
        supabase.table(target_table).delete().neq("id", "0").execute()
    except Exception as e:
        logging.error(f"❌ Failed to truncate table {target_table}: {e}")
        raise

    logging.info(f"📤 Inserting {len(normalized_data)} normalized records into {target_table}")
    if normalized_data:
        try:
            chunk_size = 100
            for i in range(0, len(normalized_data), chunk_size):
                chunk = normalized_data[i:i + chunk_size]
                supabase.table(target_table).insert(chunk).execute()
            logging.info(f"✅ Normalization complete for {target_table}")
        except Exception as e:
            logging.error(f"❌ Failed to insert records into {target_table}: {e}")
            raise
    else:
        logging.warning(f"⚠️ No records to insert into {target_table}")

def run_normalization():
    normalize("doorloop_raw_properties", "doorloop_normalized_properties", transform_property)

if __name__ == "__main__":
    run_normalization()
