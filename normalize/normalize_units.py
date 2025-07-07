
import logging
from supabase_ingest_client import upsert_records
from doorloop_client import get_raw_records
from helpers.unit_helpers import extract_unit_fields

def normalize_units():
    logging.info("🚧 Normalizing units...")
    raw_units = get_raw_records("doorloop_raw_units")
    normalized_units = [extract_unit_fields(record) for record in raw_units if record]
    logging.info(f"🔁 Upserting {len(normalized_units)} normalized units...")
    upsert_records("units", normalized_units)
