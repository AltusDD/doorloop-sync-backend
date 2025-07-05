import logging # cite: 1
import os # cite: 1
from doorloop_client import DoorLoopClient # cite: 1
from supabase_client import SupabaseClient # cite: 1
from supabase_schema_manager import SupabaseSchemaManager # cite: 1

# Set up logging # cite: 1
logging.basicConfig(level=logging.INFO) # cite: 1
logger = logging.getLogger(__name__) # cite: 1

# Load environment variables or define manually for testing # cite: 1
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY") # cite: 1
DOORLOOP_API_BASE_URL = os.getenv("DOORLOOP_API_BASE_URL", "https://api.doorloop.com/v1") # cite: 1
SUPABASE_URL = os.getenv("SUPABASE_URL") # cite: 1
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY") # cite: 1

if not all([DOORLOOP_API_KEY, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY]): # cite: 1
    raise EnvironmentError("❌ Required environment variables missing. Check API keys and Supabase config.") # cite: 1

# Initialize clients # cite: 1
dl_client = DoorLoopClient(api_key=DOORLOOP_API_KEY, base_url=DOORLOOP_API_BASE_URL) # cite: 1
sb_client = SupabaseClient( # cite: 1
    url=os.getenv("SUPABASE_URL"), # cite: 1
    service_role_key=os.getenv("SUPABASE_SERVICE_ROLE_KEY") # cite: 1
)
# CORRECTED LINE: Pass supabase_url and service_role_key directly
schema_manager = SupabaseSchemaManager(
    supabase_url=SUPABASE_URL,
    service_role_key=SUPABASE_SERVICE_ROLE_KEY
)

# Define the raw sync targets (endpoint → table name) # cite: 1
SYNC_TARGETS = { # cite: 1
    "accounts": "doorloop_raw_accounts", # cite: 1
    "users": "doorloop_raw_users", # cite: 1
    "properties": "doorloop_raw_properties", # cite: 1
    "units": "doorloop_raw_units", # cite: 1
    "leases": "doorloop_raw_leases", # cite: 1
    "tenants": "doorloop_raw_tenants", # cite: 1
    "lease-payments": "doorloop_raw_lease_payments", # cite: 1
    "lease-charges": "doorloop_raw_lease_charges", # cite: 1
    "lease-credits": "doorloop_raw_lease_credits", # cite: 1
    "tasks": "doorloop_raw_tasks", # cite: 1
    "owners": "doorloop_raw_owners", # cite: 1
    "vendors": "doorloop_raw_vendors", # cite: 1
    "expenses": "doorloop_raw_expenses", # cite: 1
    "vendor-bills": "doorloop_raw_vendor_bills", # cite: 1
    "vendor-credits": "doorloop_raw_vendor_credits", # cite: 1
    "communications": "doorloop_raw_communications", # cite: 1
    "notes": "doorloop_raw_notes", # cite: 1
    "files": "doorloop_raw_files", # cite: 1
    "portfolios": "doorloop_raw_portfolios", # cite: 1
    "lease-returned-payments": "doorloop_raw_lease_reversed_payments" # cite: 1
} # cite: 1

# Sync all raw data into raw tables # cite: 1
for endpoint, table in SYNC_TARGETS.items(): # cite: 1
    try: # cite: 1
        logger.info(f"🔄 Syncing {endpoint} into {table}...") # cite: 1
        records = dl_client.fetch_all(endpoint) # cite: 1
        if not records: # cite: 1
            logger.warning(f"⚠️ No records returned from {endpoint}") # cite: 1
            continue # cite: 1
        schema_manager.ensure_raw_table_exists(table) # cite: 1
        sb_client.insert_raw_records(table, records, source_endpoint=endpoint) # cite: 1
    except Exception as e: # cite: 1
        logger.exception(f"🔥 Failed to sync {endpoint} → {table}: {str(e)}") # cite: 1