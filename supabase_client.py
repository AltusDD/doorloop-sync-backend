import os
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def insert_raw_data(payload):
    if not isinstance(payload, list) or len(payload) == 0:
        print(f"⚠️ insert_raw_data was called with empty or invalid data: {type(payload)}")
        return

    endpoint = payload[0].get("endpoint", "unknown")  # fallback label
    table_name = endpoint.replace("/", "") + "_raw"

    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json"
    }

    for item in payload:
        item["doorloop_id"] = item.get("id")  # include DoorLoop ID for tracking

    url = f"{SUPABASE_URL}/rest/v1/{table_name}"

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 201:
        print(f"✅ Raw data inserted into {table_name}")
    else:
        print(f"❌ Failed to insert data into {table_name}: {response.status_code} {response.text}")
