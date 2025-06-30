import os
import requests
import json

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def insert_raw_data(data, endpoint):
    if not data:
        print(f"⚠️ insert_raw_data was called with empty or invalid data: {type(data)}")
        return

    # Ensure data is always a list
    if isinstance(data, dict):
        data = [data]

    # Wrap the records into our normalized schema
    payload = []
    for item in data:
        payload.append({
            "doorloop_id": item.get("id"),
            "endpoint": endpoint,
            "raw": json.dumps(item),
        })

    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
    }

    response = requests.post(
        f"{SUPABASE_URL}/rest/v1/doorloop_raw",
        headers=headers,
        data=json.dumps(payload),
    )

    if response.status_code >= 200 and response.status_code < 300:
        print(f"✅ Inserted into Supabase: {endpoint}")
    else:
        print(f"❌ Failed to insert into Supabase: {response.status_code} - {response.text}")
