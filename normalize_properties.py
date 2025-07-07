import os
import logging
from supabase import create_client, Client

# ✅ Supabase connection setup (read from environment directly)
SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_SERVICE_ROLE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# ✅ Transformation logic from public.properties → normalized.properties
def normalize_properties():
    logging.info("🔄 Starting normalization from public.properties to normalized.properties...")

    response = supabase.table("public.properties").select("*").execute()
    if not response.data:
        logging.warning("⚠️ No data found in public.properties.")
        return

    records = []
    for row in response.data:
        transformed = {
            "doorloop_id": row.get("doorloop_id"),
            "name": row.get("name"),
            "property_type": row.get("type"),
            "class": row.get("class"),
            "status": row.get("active"),
            "address_street1": row.get("address_street1"),
            "address_city": row.get("address_city"),
            "address_state": row.get("address_state"),
            "address_zip": row.get("address_zip"),
            "address_country": row.get("address_country"),
            "lat": row.get("address_lat"),
            "lng": row.get("address_lng"),
            "purchase_price": row.get("purchase_price"),
            "purchase_date": row.get("purchase_date"),
            "current_value": row.get("current_value"),
            "unit_count": row.get("num_active_units"),
            "bedroom_count": row.get("bedroom_count"),
            "manager_id": row.get("manager_id"),
            "external_id": row.get("external_id"),
            "created_at_raw": row.get("created_at"),
            "updated_at_raw": row.get("updated_at"),
            "_raw_payload": row.get("_raw_payload"),
        }
        records.append(transformed)

    if records:
        supabase.table("normalized.properties").upsert(records).execute()
        logging.info(f"✅ Normalized {len(records)} properties into normalized.properties.")
    else:
        logging.warning("⚠️ No records to upsert.")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    normalize_properties()
