import os
import logging
from doorloop_client import DoorLoopClient
from supabase_client import SupabaseClient

logging.basicConfig(level=logging.INFO)

# 🔐 Environment Variables
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# ✅ Initialize Clients
dl_client = DoorLoopClient(api_key=DOORLOOP_API_KEY)
sb_client = SupabaseClient(url=SUPABASE_URL, service_role_key=SUPABASE_SERVICE_ROLE_KEY)

# 🔁 Raw Endpoint to Table Mapping
RAW_ENDPOINTS = {
    "accounts": "doorloop_raw_accounts",
    "users": "doorloop_raw_users",
    "properties": "doorloop_raw_properties",
    "units": "doorloop_raw_units",
    "leases": "doorloop_raw_leases",
    "tenants": "doorloop_raw_tenants",
    "lease-payments": "doorloop_raw_lease_payments",
    "lease-charges": "doorloop_raw_lease_charges",
    "lease-credits": "doorloop_raw_lease_credits",
    "tasks": "doorloop_raw_tasks",
    "owners": "doorloop_raw_owners",
    "vendors": "doorloop_raw_vendors",
    "expenses": "doorloop_raw_expenses",
    "vendor-bills": "doorloop_raw_vendor_bills",
    "vendor-credits": "doorloop_raw_vendor_credits",
    "communications": "doorloop_raw_communications",
    "notes": "doorloop_raw_notes",
    "files": "doorloop_raw_files",
    "portfolios": "doorloop_raw_portfolios",
    "lease-returned-payments": "doorloop_raw_lease_reversed_payments"
}

# 🚀 Start Sync
logging.info("🔁 Starting full DoorLoop → Supabase raw sync")

for endpoint, table in RAW_ENDPOINTS.items():
    logging.info(f"🔄 Syncing endpoint: {endpoint} → table: {table}")
    try:
        records = dl_client.fetch_all(endpoint)
        wrapped_records = [
            {
                "id": record.get("id"),
                "payload_json": record,
                "endpoint": endpoint
            }
            for record in records if record.get("id")
        ]
        sb_client.upsert_data(table, wrapped_records)
    except Exception as e:
        logging.error(f"❌ Sync failed for {endpoint}: {e}")

logging.info("✅ Raw sync complete")
