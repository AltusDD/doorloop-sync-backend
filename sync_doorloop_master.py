# sync_doorloop_master.py

import os
from doorloop_client import fetch_all_entities
from supabase_client import insert_raw_data

DOORLOOP_ENTITIES = [
    "properties",
    "units",
    "leases",
    "tenants",
    "owners",
    "tasks",
    "vendors",
    "users",
    "accounts",
    "attachments",
    "applications",
    "communications",
    "expenses",
    "files",
    "activity_logs",
    "reports",
    "recurring_charges",
    "recurring_credits",
    "lease_charges",
    "lease_credits",
    "lease_payments",
    "notes",
    "portfolios"
]

def main():
    for entity in DOORLOOP_ENTITIES:
        print(f"🔄 Syncing {entity}...")
        try:
            data = fetch_all_entities(entity)
            print(f"✅ Retrieved {len(data)} records for {entity}")
            insert_raw_data(entity, data)
            print(f"📥 Inserted {entity} records into Supabase.")
        except Exception as e:
            print(f"❌ Failed syncing {entity}: {e}")

if __name__ == "__main__":
    main()
