
import logging
import os
from supabase_ingest_client import SupabaseIngestClient
from doorloop_client import get_raw_records
from helpers.property_helpers import (
    flatten_property_record,
    extract_property_owners,
    extract_property_pictures,
)

logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
client = SupabaseIngestClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def normalize_properties():
    logger.info("🔁 Normalizing properties from doorloop_raw_properties...")

    raw_records = get_raw_records("doorloop_raw_properties")
    if not raw_records:
        logger.warning("⚠️ No raw property records found.")
        return

    normalized_properties = []
    property_owners_links = []
    property_pictures_links = []

    for record in raw_records:
        try:
            flattened = flatten_property_record(record)
            normalized_properties.append(flattened)

            property_owners_links.extend(extract_property_owners(record))
            property_pictures_links.extend(extract_property_pictures(record))

        except Exception as e:
            logger.error(f"❌ Failed to normalize record ID={record.get('id')}: {e}")

    client.upsert_data("properties", normalized_properties)
    client.upsert_data("property_owners", property_owners_links)
    client.upsert_data("property_pictures", property_pictures_links)

    logger.info(f"✅ Normalized {len(normalized_properties)} properties.")
