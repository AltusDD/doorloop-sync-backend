import os
import json
import logging
from supabase import create_client, SupabaseException

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLES_TO_NORMALIZE = [
    "doorloop_normalized_properties",
    "doorloop_normalized_units",
    "doorloop_normalized_leases",
    "doorloop_normalized_tenants",
    "doorloop_normalized_owners"
]

def normalize(table):
    logger.info(f"📥 Fetching data from doorloop_raw_{table}...")
    raw_data = supabase.table(f"doorloop_raw_{table}").select("*").execute().data

    if not raw_data:
        logger.warning(f"⚠️ No data found in doorloop_raw_{table}, skipping.")
        return

    logger.info(f"📊 Normalizing {len(raw_data)} records from doorloop_raw_{table} → doorloop_normalized_{table}")

    norm_table = f"doorloop_normalized_{table}"

    logger.info(f"🧹 Deleting all data from {norm_table}...")
    supabase.table(norm_table).delete().neq("id", "00000000-0000-0000-0000-000000000000").execute()

    for row in raw_data:
        normalized_row = {k: (json.dumps(v) if isinstance(v, (dict, list)) else v) for k, v in row.items()}
        supabase.table(norm_table).insert(normalized_row).execute()

def run_normalization():
    logger.info("🚀 Starting normalization process...")
    for table in ["properties", "units", "leases", "tenants", "owners"]:
        try:
            normalize(table)
        except Exception as e:
            logger.error(f"❌ Failed to normalize table {table}: {str(e)}")

if __name__ == "__main__":
    run_normalization()