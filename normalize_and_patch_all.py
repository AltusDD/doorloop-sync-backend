import os
import json
import uuid
import logging
from supabase import create_client, Client
from dotenv import load_dotenv
import re
import psycopg2

# --- Set up Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

# --- Database Connection Details ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logging.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")
    exit(1)

# Extract connection details from SUPABASE_URL for psycopg2
# Example: "postgresql://postgres:password@db.xyz.supabase.co:5432/postgres"
db_url_parts = SUPABASE_URL.split('@')[1].split(':')
db_host = db_url_parts[0]
db_port = db_url_parts[1].split('/')[0]
db_name = db_url_parts[1].split('/')[1]
db_user = SUPABASE_URL.split('//')[1].split(':')[0]
db_password = SUPABASE_SERVICE_ROLE_KEY

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# --- Helper Function to Convert from CamelCase to SnakeCase ---
def camel_to_snake(name: str) -> str:
    """Converts a CamelCase string to a snake_case string."""
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()

# --- Helper Function for ID Conversion ---
def mongodb_id_to_uuid(mongo_id: str) -> str:
    """Converts a MongoDB-style ObjectId to a UUIDv5 deterministically."""
    if not isinstance(mongo_id, str) or len(mongo_id) != 24:
        return None
    return str(uuid.uuid5(uuid.NAMESPACE_OID, mongo_id))

# --- Definitive Schema for doorloop_normalized_properties ---
# This is the single source of truth. Your script will now enforce this.
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

def ensure_schema_is_correct(table_name: str):
    """Checks and corrects schema for a given table using psycopg2."""
    logging.info(f"🔍 Enforcing schema consistency for table: {table_name}")
    try:
        conn = psycopg2.connect(
            host=db_host,
            port=db_port,
            dbname=db_name,
            user=db_user,
            password=db_password
        )
        cur = conn.cursor()

        expected_columns = TARGET_SCHEMA[table_name]
        cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table_name}'")
        existing_columns = [row[0] for row in cur.fetchall()]

        for col, col_type in expected_columns.items():
            if col not in existing_columns:
                logging.warning(f"⚠️ Column '{col}' not found. Adding with type '{col_type}'.")
                cur.execute(f"ALTER TABLE public.{table_name} ADD COLUMN {col} {col_type};")
                conn.commit()
                logging.info(f"✅ Successfully added column '{col}'.")
        
        cur.close()
        conn.close()
        logging.info("✅ Schema enforcement complete.")

    except Exception as e:
        logging.error(f"❌ Failed to enforce schema for {table_name}: {e}")
        raise


# --- Transformation Function for Properties ---
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
    
    transformed_data["status"] = "active" if transformed_data.get("active") else "inactive"
    
    if 'num_active_units' in transformed_data:
        transformed_data['unit_count'] = transformed_data.pop('num_active_units')
    
    if 'type' in transformed_data:
        transformed_data['property_type'] = transformed_data.pop('type')

    # Remove any keys not in our target schema to prevent insertion errors
    # This is the "bulletproof" part for data
    return {k: v for k, v in transformed_data.items() if k in TARGET_SCHEMA['doorloop_normalized_properties']}


# --- Normalization and Insertion Logic ---
def normalize(source_table: str, target_table: str, transform_function: callable):
    ensure_schema_is_correct(target_table)
    
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