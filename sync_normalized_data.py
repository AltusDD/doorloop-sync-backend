# force trigger
import os
import uuid
from supabase import create_client, Client
from dotenv import load_dotenv
import logging # Added for better logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logging.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def mongodb_id_to_uuid(mongo_id: str) -> str:
    """Converts a MongoDB-style ObjectId to a UUIDv5 deterministically."""
    if not isinstance(mongo_id, str) or len(mongo_id) != 24:
        logging.warning(f"Invalid MongoDB ObjectId format received for conversion: {mongo_id}. Returning as is.")
        return mongo_id # Return as is if not a valid MongoID format, let DB handle
    return str(uuid.uuid5(uuid.NAMESPACE_OID, mongo_id))

def sync_table(raw_table: str, normalized_table: str, field_map: dict):
    logging.info(f"🔄 Syncing {raw_table} → {normalized_table}")
    
    try:
        response = supabase.table(raw_table).select("*").execute()
        raw_data = response.data
        logging.info(f"Fetched {len(raw_data)} records from {raw_table}.")
    except Exception as e:
        logging.error(f"Failed to fetch data from {raw_table}: {e}")
        raise # Re-raise to fail the workflow

    records_to_insert = []
    for record in raw_data:
        transformed = {}
        for raw_field, normalized_column_name in field_map.items():
            if raw_field in record:
                value = record[raw_field]
                
                # Special handling for 'id' and other foreign key DoorLoop IDs
                if normalized_column_name == 'id' and isinstance(value, str) and len(value) == 24:
                    # 'id' column in normalized_table will store the UUIDv5
                    transformed['id'] = mongodb_id_to_uuid(value)
                    # 'doorloop_id' column in normalized_table will store the original MongoDB ObjectId (TEXT type in DB)
                    transformed['doorloop_id'] = value 
                elif normalized_column_name.endswith('_id') and isinstance(value, str) and len(value) == 24:
                    # For FKs like 'owner_id', 'manager_id', etc.
                    # Assuming these columns in the DB are also UUID types storing converted IDs
                    transformed[normalized_column_name] = mongodb_id_to_uuid(value)
                else:
                    # Direct mapping for other fields
                    transformed[normalized_column_name] = value
            else:
                # Log missing fields if necessary, or assign default/null
                logging.debug(f"Field '{raw_field}' not found in record from {raw_table}. Skipping or defaulting.")
                # transformed[normalized_column_name] = None # Uncomment if you want to explicitly set to None

        records_to_insert.append(transformed)

    if records_to_insert:
        try:
            logging.info(f"Attempting to insert {len(records_to_insert)} records into {normalized_table}...")
            # 'upsert=True' requires a primary key, which should be 'id' (UUIDv5) in your normalized table
            supabase.table(normalized_table).insert(records_to_insert, upsert=True).execute()
            logging.info(f"✅ Successfully synced {len(records_to_insert)} records to {normalized_table}.")
        except Exception as e:
            logging.error(f"❌ Failed to insert/upsert records into {normalized_table}: {e}")
            raise # Re-raise to fail the workflow
    else:
        logging.info(f"No records to insert into {normalized_table}.")

# --- Define Mappings ---
# This field_map defines how raw_field (from DoorLoop API) maps to normalized_column_name (in Supabase table)
# Crucially, 'id' from DoorLoop maps to 'id' in the Python dict, which then gets split into
# 'id' (UUIDv5) and 'doorloop_id' (original MongoID string) before insertion.

# Field map for Properties
properties_field_map = {
    "id": "id",              # This 'id' will become UUIDv5 and generate 'doorloop_id' (TEXT)
    "name": "name",
    "type": "type",
    "addressStreet1": "address_street1", # Example of flattening address.street1 -> address_street1
    "addressCity": "address_city",
    "addressState": "address_state",
    "addressZip": "address_zip",
    "owners": "owners_json", # Store complex JSONB fields as JSONB
    "managerId": "manager_id", # This will be converted to UUIDv5 by the logic
}

# --- Execute Sync for Properties ---
sync_table("doorloop_raw_properties", "doorloop_normalized_properties", properties_field_map)

# --- Add other tables here ---
# Example for Leases (assuming leases have 'id', 'tenant_id', 'unit_id', 'property_id' as DoorLoop IDs)
# leases_field_map = {
#     "id": "id",
#     "name": "name",
#     "start": "start_date",
#     "end": "end_date",
#     "tenantId": "tenant_id",     # This will be converted to UUIDv5 by the logic
#     "unitId": "unit_id",         # This will be converted to UUIDv5 by the logic
#     "propertyId": "property_id", # This will be converted to UUIDv5 by the logic
#     "tenants": "tenants_json",   # Store complex JSONB for audit/future processing
#     "units": "units_json",       # Store complex JSONB for audit/future processing
# }
# sync_table("doorloop_raw_leases", "doorloop_normalized_leases", leases_field_map)