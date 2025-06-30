import json
import requests

def upsert_raw_doorloop_data(endpoint, records, supabase_url, supabase_key):
    """
    Sends raw DoorLoop data to the corresponding Supabase table using upsert logic.
    Example table: doorloop_raw_leases, doorloop_raw_units, etc.
    """

    url = f"{supabase_url}/rest/v1/doorloop_raw_{endpoint}"
    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    print(f"🚀 Sending {len(records)} records to Supabase table: doorloop_raw_{endpoint}")

    success_count = 0
    fail_count = 0

    for idx, record in enumerate(records):
        try:
            payload = json.dumps(record)
            response = requests.post(url, headers=headers, data=payload)

            if response.status_code >= 400:
                print(f"❌ Record {idx + 1}: Failed with status {response.status_code}")
                print(f"   → Error: {response.text}")
                fail_count += 1
            else:
                print(f"✅ Record {idx + 1}: Inserted successfully")
                success_count += 1

        except Exception as e:
            print(f"🔥 Record {idx + 1}: Exception occurred → {e}")
            fail_count += 1

    print(f"\n🧾 Insert Summary for `{endpoint}`:")
    print(f"   ✅ Successes: {success_count}")
    print(f"   ❌ Failures: {fail_count}")
