
import os
import requests

SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

def post_to_supabase(endpoint: str, data: list):
    print(f"🔄 Posting {len(data)} records to Supabase endpoint: {endpoint}")
    if data:
        print(f"📌 Sample record: {data[0]}")
    else:
        print("⚠️ No data to post")

    url = f"{SUPABASE_URL}/rest/v1/{endpoint}"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }
    response = requests.post(url, json=data, headers=headers)
    print(f"✅ Supabase Response Status: {response.status_code}")
    print(f"📝 Supabase Response Body: {response.text}")
    response.raise_for_status()
    return response.json()
