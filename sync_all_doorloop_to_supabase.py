
import os
import logging
from doorloop_client import DoorLoopClient
from supabase_client import SupabaseClient
from supabase_schema_manager import SupabaseSchemaManager

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# ✅ Set up environment variables
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")

if not all([DOORLOOP_API_KEY, DOORLOOP_API_BASE_URL, SUPABASE_SERVICE_ROLE_KEY, SUPABASE_URL]):
    raise EnvironmentError("Missing one or more required environment variables.")

# ✅ Initialize clients
dl_client = DoorLoopClient(api_key=DOORLOOP_API_KEY, base_url=DOORLOOP_API_BASE_URL)
sb_client = SupabaseClient(supabase_url=SUPABASE_URL, service_role_key=SUPABASE_SERVICE_ROLE_KEY)
schema_manager = SupabaseSchemaManager(sb_client)

# ✅ Define the list of endpoints to sync
endpoints = [
    "accounts",
    "users",
    "properties",
    "units",
    "leases",
    "tenants",
    "lease-payments",
    "lease-charges",
    "lease-credits",
    "tasks",
    "owners",
    "vendors",
    "expenses",
    "vendor-bills",
    "vendor-credits",
    "communications",
    "notes",
    "files",
    "portfolios",
    "lease-returned-payments"
]

# ✅ Sync each endpoint to its corresponding doorloop_raw_ table
for endpoint in endpoints:
    logger.info(f"🔄 Syncing raw endpoint: {endpoint}")

    try:
        records = dl_client.fetch_all(endpoint)
        logger.info(f"✅ Retrieved {len(records)} records from DoorLoop /{endpoint}")

        raw_table = f"doorloop_raw_{endpoint.replace('-', '_')}"

        schema_manager.ensure_raw_table(raw_table)  # ensures table exists and has correct structure

        payloads = [{
            "id": r.get("id") or r.get("_id"),
            "payload_json": r,
            "endpoint": endpoint
        } for r in records]

        sb_client.upsert_raw_data(raw_table, payloads)
        logger.info(f"📥 Upserted {len(payloads)} into {raw_table}")

    except Exception as e:
        logger.exception(f"🔥 Exception while syncing {endpoint}: {e}")
