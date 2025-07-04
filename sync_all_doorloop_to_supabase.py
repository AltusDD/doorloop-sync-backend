
import logging
from doorloop_client import DoorLoopClient
from supabase_client import SupabaseClient
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL")  # should be https://app.doorloop.com/api
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not all([DOORLOOP_API_KEY, DOORLOOP_API_BASE_URL, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY]):
    logger.error("❌ ❌ Configuration Error: Missing one or more required environment variables.")
    exit(1)

logger.info("✅ 🚀 Starting DoorLoop → Supabase sync")

doorloop = DoorLoopClient(DOORLOOP_API_KEY, DOORLOOP_API_BASE_URL)
supabase = SupabaseClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

endpoints = [
    "accounts", "users", "properties", "units", "leases", "tenants",
    "lease-payments", "lease-charges", "lease-credits", "tasks",
    "owners", "vendors", "expenses", "vendor-bills", "vendor-credits",
    "communications", "notes", "files", "portfolios"
]

for endpoint in endpoints:
    logger.info(f"✅ 🔄 Syncing endpoint: {endpoint}")
    try:
        data = doorloop.fetch_all(endpoint)
        supabase.insert_records(endpoint.replace("-", "_"), data)
    except Exception as e:
        logger.error(f"❌ ❌ Failed syncing {endpoint}: {e}")
