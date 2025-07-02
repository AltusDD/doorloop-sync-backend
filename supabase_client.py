import os
import requests

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def upsert_data_to_supabase(endpoint, data):
    if not data:
        print(f"⚠️ No data to upsert for {endpoint}")
        return

    table_name = endpoint.strip("/").replace("-", "_")

    url = f"{SUPABASE_URL}/rest/v1/{table_name}?on_conflict=id"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    try:
        print(f"📤 Upserting {len(data)} records to Supabase table: {table_name}")
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        print(f"✅ Upsert complete for {table_name}.")
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to upsert data to {table_name}: {e}")
        print(f"🔁 Response Text: {response.text if 'response' in locals() else 'N/A'}")