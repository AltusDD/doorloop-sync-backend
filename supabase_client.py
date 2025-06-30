import requests
import hashlib
import json

def hash_payload(payload):
    return hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()

def upsert_raw_doorloop_data(endpoint, records, supabase_url, service_role_key):
    table_name = f"doorloop_raw_{endpoint.strip('/').replace('/', '_')}"
    headers = {
        "apikey": service_role_key,
        "Authorization": f"Bearer {service_role_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    url = f"{supabase_url}/rest/v1/{table_name}"

    for i, record in enumerate(records, start=1):
        payload = {
            "endpoint": endpoint,
            "entity_dl_id": record.get("id"),
            "payload_json": record,
            "payload_hash": hash_payload(record)
        }

        response = requests.post(url, headers=headers, json=payload)

        if not response.ok:
            print(f"❌ Record {i}: Failed with status {response.status_code}")
            print(f"   → Error: {response.text}")
        else:
            print(f"✅ Record {i}: Inserted successfully.")
