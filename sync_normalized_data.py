# sync_normalized_data.py - Extreme Tracer Version
import os
import uuid
from supabase import create_client, Client
from dotenv import load_dotenv
import logging
import json # Import json for pretty printing

# Configure logging for maximum verbosity
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logging.error("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")
    exit(1)

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def mongodb_id_to_uuid(mongo_id: str) -> str:
    """Converts a MongoDB-style ObjectId to a UUIDv5 deterministically."""
    # TRACER: Log input and type before conversion attempt
    logging.debug(f"  [UUID Conv] Input: '{mongo_id}' (Type: {type(mongo_id)}, Len: {len(str(mongo_id)) if isinstance(mongo_id, str) else 'N/A'})")
    
    if not isinstance(mongo_id, str) or len(mongo_id) != 24:
        logging.warning(f"  [UUID Conv] Skipped: Non-24-char string or non-string ID. Returning as is.")
        return mongo_id # Return as is if not a valid MongoID format, let DB handle
    
    try:
        converted_uuid = str(uuid.uuid5(uuid.NAMESPACE_OID, mongo_id))
        logging.debug(f"  [UUID Conv] SUCCESS: Converted '{mongo_id}' -> '{converted_uuid}'")
        return converted_uuid
    except Exception as e:
        logging.error(f"  [UUID Conv] ERROR: Failed to convert '{mongo_id}' to UUID: {e}. Returning original.")
        return mongo_id # Fallback to original if conversion fails for unexpected reason

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
    
    record_counter = 0 
    for record in raw_data:
        record_counter += 1
        transformed = {}
        
        logging.debug(f"  [Transforming Record #{record_counter}] Raw: {json.dumps(record, indent=2)}")

        for raw_field, normalized_column_name in field_map.items():
            value = record.get(raw_field) # Use .get() to avoid KeyError if field is missing

            # TRACER: Log each field's transformation
            logging.debug(f"    [Field Map] '{raw_field}' -> '{normalized_column_name}' (Raw Value: '{value}')")

            # Special handling for 'id' and other foreign key DoorLoop IDs
            if normalized_column_name == 'id' and isinstance(value, str) and len(value) == 24:
                transformed['id'] = mongodb_id_to_uuid(value)
                transformed['doorloop_id'] = value 
                logging.debug(f"      [ID Handling] 'id' mapped to UUID '{transformed['id']}' and 'doorloop_id' as TEXT '{transformed['doorloop_id']}'")
            elif normalized_column_name.endswith('_id') and isinstance(value, str) and len(value) == 24:
                # This catches manager_id, property_id, unit_id, tenant_id etc.
                transformed[normalized_column_name] = mongodb_id_to_uuid(value)
                logging.debug(f"      [FK ID Handling] '{normalized_column_name}' mapped to UUID '{transformed[normalized_column_name]}'")
            else:
                transformed[normalized_column_name] = value
                logging.debug(f"      [Direct Map] '{normalized_column_name}' set to '{transformed[normalized_column_name]}'")
        
        records_to_insert.append(transformed)
        logging.debug(f"  [Transformed Record #{record_counter}] Final: {json.dumps(transformed, indent=2)}")

    if records_to_insert:
        try:
            logging.info(f"Attempting to insert {len(records_to_insert)} records into {normalized_table}...")
            
            # TRACER: Log a sample of records for the specific table being inserted into
            if records_to_insert:
                logging.info(f"Sample for {normalized_table} (Recs 1-2): {json.dumps(records_to_insert[:2], indent=2)}")
                if len(records_to_insert) > 2:
                    logging.info(f"Sample for {normalized_table} (Last 2 Recs): {json.dumps(records_to_insert[-2:], indent=2)}")

            # THIS IS THE LINE THAT WILL THROW THE APIError
            supabase.table(normalized_table).insert(records_to_insert, upsert=True).execute()
            logging.info(f"✅ Successfully synced {len(records_to_insert)} records to {normalized_table}.")
        except Exception as e:
            # TRACER: Enhanced error logging to pinpoint the exact table and sample data
            logging.error(f"❌ FAILED to insert/upsert records into {normalized_table}: {e}")
            logging.error(f"Error Type: {type(e).__name__}, Message: {getattr(e, 'message', str(e))}")
            if hasattr(e, 'response') and e.response:
                logging.error(f"Supabase/PostgREST API Error Response: {json.dumps(e.response, indent=2)}")
            logging.error(f"Error occurred while processing records for table: {normalized_table}")
            logging.error(f"First 5 problematic records (from batch): {json.dumps(records_to_insert[:5], indent=2)}")
            
            # If the error message from Supabase doesn't specify which record, this is best effort.
            raise # Re-raise to fail the workflow
    else:
        logging.info(f"No records to insert into {normalized_table}.")

# --- Define Mappings ---
properties_field_map = {
    "id": "id",
    "name": "name",
    "type": "type",
    "addressStreet1": "address_street1",
    "addressCity": "address_city",
    "addressState": "address_state",
    "addressZip": "address_zip",
    "owners": "owners_json",
    "managerId": "manager_id", # This will be converted to UUIDv5 by the logic
}

# --- Execute Sync for Properties ---
sync_table("doorloop_raw_properties", "doorloop_normalized_properties", properties_field_map)

# --- Add other sync_table calls here, ensuring their field maps are also correct ---
# Example:
# leases_field_map = {
#     "id": "id",
#     "name": "name",
#     "start": "start_date",
#     "tenantId": "tenant_id", # Converted to UUIDv5
#     "unitId": "unit_id",     # Converted to UUIDv5
#     "propertyId": "property_id", # Converted to UUIDv5
#     # ... other fields ...
# }
# sync_table("doorloop_raw_leases", "doorloop_normalized_leases", leases_field_map)