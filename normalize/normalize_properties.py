
import os
from doorloop_client import DoorLoopClient
from supabase_ingest_client import SupabaseIngestClient
from helpers.property_helpers import extract_property_fields, extract_property_owners, extract_property_pictures

# Load secrets
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Initialize clients
dl_client = DoorLoopClient(api_key=DOORLOOP_API_KEY, base_url=DOORLOOP_API_BASE_URL)
sb_client = SupabaseIngestClient(supabase_url=SUPABASE_URL, service_role_key=SUPABASE_SERVICE_ROLE_KEY)

def normalize_properties():
    print("🔄 Normalizing properties from DoorLoop API...")

    raw_properties = dl_client.fetch_all("properties")
    if not raw_properties:
        print("⚠️ No property records fetched.")
        return

    # Transform data
    normalized_properties = [extract_property_fields(p) for p in raw_properties]
    property_owners_links = [link for p in raw_properties for link in extract_property_owners(p)]
    property_pictures_links = [link for p in raw_properties for link in extract_property_pictures(p)]

    # Upload to Supabase
    sb_client.upsert_data("properties", normalized_properties)
    sb_client.upsert_data("property_owners", property_owners_links)
    sb_client.upsert_data("property_pictures", property_pictures_links)

    print("✅ Properties normalization and upload complete.")
