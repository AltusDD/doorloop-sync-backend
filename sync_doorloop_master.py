import os
from dotenv import load_dotenv
from doorloop_client import fetch_all_entities
from supabase_client import upsert_raw_doorloop_data  # ✅ Corrected function name

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
DOORLOOP_API_KEY = os.getenv("DOORLOOP_API_KEY")

# Define all DoorLoop endpoints and their destination Supabase tables
ENTITY_ENDPOINT_MAP = {
    "properties": "doorloop_raw_properties",
    "units": "doorloop_raw_units",
    "leases": "doorloop_raw_leases",
    "tenants": "doorloop_raw_tenants",
    "owners": "doorloop_raw_owners",
    "portfolios": "doorloop_raw_portfolios",
    "tasks": "doorloop_raw_tasks",
    "work_orders": "doorloop_raw_work_orders",
    "lease_charges": "doorloop_raw_lease_charges",
    "lease_payments": "doorloop_raw_lease_payments",
    "lease_credits": "doorloop_raw_lease_credits",
    "communications": "doorloop_raw_communications",
    "vendors": "doorloop_raw_vendors",
    "users": "doorloop_raw_users",
    "activity_logs": "doorloop_raw_activity_logs",
    "applications": "doorloop_raw_applications",
    "attachments": "doorloop_raw_attachments",
    "accounts": "doorloop_raw_accounts",
    "expenses": "doorloop_raw_expenses",
    "notes": "doorloop_raw_notes",
    "reports": "doorloop_raw_reports",
    "recurring_charges": "doorloop_raw_recurring_charges",
    "recurring_credits": "doorloop_raw_recurring_credits",
    "insurance_policies": "doorloop_raw_insurance_policies",
    "files": "doorloop_raw_files"
}

def run_master_sync():
    for endpoint, table_name in ENTITY_ENDPOINT_MAP.items():
        print(f"🔄 Syncing {endpoint} → {table_name}")
        try:
            records = fetch_all_entities(DOORLOOP_API_KEY, endpoint)
            upsert_raw_doorloop_data(endpoint, records, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
            print(f"✅ Synced {len(records)} records to {table_name}")
        except Exception as e:
            print(f"❌ Failed to sync {endpoint}: {str(e)}")

if __name__ == "__main__":
    run_master_sync()
