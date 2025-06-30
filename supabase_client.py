import os
import requests
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def insert_raw_data(payload):
    if not isinstance(payload, list) or not payload:
        print(f"⚠️ insert_raw_data was called with invalid payload: {type(payload)} → {payload}")
        return

    sample = payload[0]
    if not isinstance(sample, dict):
        print(f"❌ Cannot process: payload[0] is not a dict → {type(sample)}")
        return

    endpoint = sample.get("endpoint", "unknown")
    table_name = endpoint.replace("/", "") + "_raw"

    # Attach doorloop_id to each item
    for item in payload:
        if isinstance(item, dict):
            item["doorloop_id"] = item.get("id")

    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json"
    }

    url = f"{SUPABASE_URL}/rest/v1/{table_name}"
    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 201:
        print(f"✅ Inserted into Supabase: {endpoint}")
    else:
        print(f"❌ Supabase insert failed ({response.status_code}): {response.text}")
