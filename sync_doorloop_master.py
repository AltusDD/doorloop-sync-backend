import os
import json
from dotenv import load_dotenv
from supabase_client import upsert_raw_doorloop_data
from doorloop_client import fetch_all_entities

load_dotenv()

# Set the list of DoorLoop endpoints you want to sync
DOORLOOP_ENDPOINTS = [
    "/properties",
    "/units",
    "/tenants",
    "/owners",
    "/leases",
    "/lease-payments",
    "/lease-charges",
    "/lease-credits",
    "/vendors",
    "/tasks",
    "/files",
    "/notes",
    "/communications",
    "/applications",
    "/inspections",
    "/insurance-policies",
    "/recurring-charges",
    "/recurring-credits",
    "/accounts",
    "/users",
    "/portfolios",
    "/reports",
    "/activity-logs",
    "/attachments",
    "/expenses"
]

def sync_all():
    for endpoint in DOORLOOP_ENDPOINTS:
        print(f"\n🚀 Syncing: {endpoint}")
        try:
            records = fetch_all_entities(endpoint)
            upsert_raw_doorloop_data(endpoint, records)
        except Exception as e:
            print(f"❌ Error syncing {endpoint}: {e}")

if __name__ == "__main__":
    sync_all()
