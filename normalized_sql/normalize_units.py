import logging
from supabase_ingest_client import upsert_records
from doorloop_client import get_raw_records
from helpers.unit_helpers import extract_unit_fields

def normalize_units():
    logging.info("🧱 Normalizing units from doorloop_raw_units → units")

    raw_units = get_raw_records("doorloop_raw_units")
    if not raw_units:
        logging.warning("⚠️ No raw unit records found.")
        return

    transformed = []
    for raw in raw_units:
        try:
            transformed.append(extract_unit_fields(raw))
        except Exception as e:
            logging.error(f"❌ Error transforming unit: {e}")

    if transformed:
        upsert_records("units", transformed)
        logging.info(f"✅ Upserted {len(transformed)} unit records.")
    else:
        logging.warning("⚠️ No transformed unit records to upsert.")
