from supabase_ingest_client import upsert_records
from doorloop_client import get_raw_records
from helpers.property_helpers import extract_property_fields

def normalize_properties():
    print("🔁 Normalizing properties...")
    raw_records = get_raw_records("doorloop_raw_properties")
    cleaned_records = [extract_property_fields(record) for record in raw_records if record]
    print(f"✅ Extracted {len(cleaned_records)} cleaned property records")
    upsert_records("properties", cleaned_records)
