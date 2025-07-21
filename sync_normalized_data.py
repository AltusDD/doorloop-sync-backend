
# sync_normalized_data_patch.py
# Patch for correct DoorLoop → Supabase normalized sync

import logging
from supabase import create_client
from doorloop_client import DoorLoopClient

# Init Supabase and DoorLoop Clients (use your real secrets)
supabase_url = "YOUR_SUPABASE_URL"
supabase_key = "YOUR_SUPABASE_KEY"
supabase = create_client(supabase_url, supabase_key)

doorloop_client = DoorLoopClient(api_key="YOUR_DOORLOOP_API_KEY", base_url="https://api.doorloop.com")

def upsert_entity(table_name, records, unique_key="doorloop_id"):
    for record in records:
        if "id" in record:
            record["doorloop_id"] = record.pop("id")
        try:
            supabase.table(table_name).upsert(record, on_conflict=[unique_key]).execute()
        except Exception as e:
            logging.error(f"❌ Upsert failed for {table_name}: {e}")

def sync_properties():
    logging.info("🏢 Syncing properties...")
    props = doorloop_client.fetch_all("properties")
    upsert_entity("doorloop_normalized_properties", props)

def sync_units():
    logging.info("🏘️ Syncing units...")
    units = doorloop_client.fetch_all("units")
    for unit in units:
        if "propertyId" in unit:
            # Map DoorLoop property ID to internal UUID (you must resolve this in production)
            unit["property_id"] = resolve_internal_id("doorloop_normalized_properties", unit["propertyId"])
            del unit["propertyId"]
    upsert_entity("doorloop_normalized_units", units)

def resolve_internal_id(table, doorloop_id):
    # Utility to resolve foreign key via doorloop_id
    result = supabase.table(table).select("id").eq("doorloop_id", doorloop_id).limit(1).execute()
    if result.data:
        return result.data[0]["id"]
    else:
        logging.warning(f"⚠️ Could not resolve FK for {doorloop_id} in {table}")
        return None

def main():
    sync_properties()
    sync_units()
    # Add leases, tenants, owners similarly...

if __name__ == "__main__":
    main()
