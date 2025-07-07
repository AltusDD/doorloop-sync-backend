
import logging
from supabase_ingest_client import upsert_records
from doorloop_client import get_raw_records
from helpers.property_helpers import (
    flatten_property_record,
    extract_property_owners,
    extract_property_pictures,
)

logger = logging.getLogger(__name__)

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

            # Extract linking records
            property_owners_links.extend(extract_property_owners(record))
            property_pictures_links.extend(extract_property_pictures(record))

        except Exception as e:
            logger.error(f"❌ Failed to normalize record ID={record.get('id')}: {e}")

    upsert_records("properties", normalized_properties)
    upsert_records("property_owners", property_owners_links)
    upsert_records("property_pictures", property_pictures_links)

    logger.info(f"✅ Normalized {len(normalized_properties)} properties.")
