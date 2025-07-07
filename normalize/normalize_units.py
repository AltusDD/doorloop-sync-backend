from supabase_ingest_client import upsert_records
from doorloop_client import get_raw_records
from helpers.unit_helpers import extract_unit_fields

def normalize_units():
    print("🔁 Normalizing units...")
    raw_records = get_raw_records("doorloop_raw_units")
    cleaned_records = [extract_unit_fields(record) for record in raw_records if record]
    print(f"✅ Extracted {len(cleaned_records)} cleaned unit records")
    upsert_records("units", cleaned_records)
