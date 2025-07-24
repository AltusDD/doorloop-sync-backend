import os
import json
import uuid
import logging
from supabase import create_client, Client
from dotenv import load_dotenv

# --- Set up Logging ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logging.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# --- Helper Function for ID Conversion ---
def mongodb_id_to_uuid(mongo_id: str) -> str:
    """Converts a MongoDB-style ObjectId to a UUIDv5 deterministically."""
    if not isinstance(mongo_id, str) or len(mongo_id) != 24:
        # For cases where an ID field is null or not a valid MongoID,
        # return None for the DB to handle as a nullable UUID.
        return None
    return str(uuid.uuid5(uuid.NAMESPACE_OID, mongo_id))

# --- Transformation Function for Properties ---
def transform_property(row: dict) -> dict:
    data = row.get("data", {})
    if not data.get("id"):
        logging.warning(f"⚠️ Skipped row missing id: {row}")
        return None

    return {
        "id": mongodb_id_to_uuid(data.get("id")),         # Generates UUIDv5 for primary key
        "doorloop_id": data.get("id"),                    # Stores original MongoID
        "name": data.get("name"),
        # --- FIX: Corrected field name from 'propertyType' to 'property_type' ---
        "property_type": data.get("type"),
        # --- END FIX ---
        "address_street1": data.get("address", {}).get("street1"),
        "address_city": data.get("address", {}).get("city"),
        "address_state": data.get("address", {}).get("state"),
        "address_zip": data.get("address", {}).get("zip"),
        "manager_id": mongodb_id_to_uuid(data.get("managerId")),
        "class": data.get("class"),
        "status": "active" if data.get("active") else "inactive",
        "unitCount": data.get("numActiveUnits", 0),
        "created_at": data.get("createdAt"),
        "updated_at": data.get("updatedAt"),
        "pictures_json": data.get("pictures", []),
    }

# --- Normalization and Insertion Logic ---
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