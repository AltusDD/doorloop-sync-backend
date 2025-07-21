import os
import json
import logging
from supabase import create_client
from supabase.client import Client

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise ValueError("❌ SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set as environment variables.")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Normalized table list
tables = [
    "doorloop_normalized_properties",
    "doorloop_normalized_units",
    "doorloop_normalized_tenants",
    "doorloop_normalized_leases",
    "doorloop_normalized_owners",
]

# Raw → Normalized mapping
raw_sources = {
    "doorloop_raw_properties": "doorloop_normalized_properties",
    "doorloop_raw_units": "doorloop_normalized_units",
    "doorloop_raw_tenants": "doorloop_normalized_tenants",
    "doorloop_raw_leases": "doorloop_normalized_leases",
    "doorloop_raw_owners": "doorloop_normalized_owners",
}

def normalize_data(table_name, records):
    normalized = []

    for record in records:
        new_record = {}
        for k, v in record.items():
            if k in ["id", "created_at", "updated_at"]:
                continue  # skip raw IDs and Supabase timestamps
            new_key = k.lower()
            val = json.dumps(v) if isinstance(v, (dict, list)) else f"'{str(v).replace(\"'\", \"''\")}'"
            new_record[new_key] = val
        normalized.append(new_record)

    return normalized

def run_normalization():
    logger.info("🚀 Starting normalization process...")

    for raw_table, norm_table in raw_sources.items():
        logger.info(f"📥 Fetching data from {raw_table}...")
        response = supabase.table(raw_table).select("*").execute()
        records = response.data

        if not records:
            logger.warning(f"⚠️ No data found in {raw_table}. Skipping.")
            continue

        logger.info(f"📊 Normalizing {len(records)} records from {raw_table} → {norm_table}")
        normalized_records = normalize_data(norm_table, records)

        # Clear the normalized table first
        logger.info(f"🧹 Clearing existing data in {norm_table}...")
        supabase.table(norm_table).delete().neq("id", "").execute()

        # Insert normalized records
        logger.info(f"🆕 Inserting normalized records into {norm_table}...")
        for record in normalized_records:
            supabase.table(norm_table).insert(record).execute()

        logger.info(f"✅ Finished syncing {norm_table}")

    logger.info("🎉 All normalized tables processed successfully.")

if __name__ == "__main__":
    run_normalization()
