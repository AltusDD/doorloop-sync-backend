import os
import requests
import json

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

def retry_dlq_via_rest():
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json"
    }

    # Step 1: Fetch unresolved DLQ records
    resp = requests.get(
        f"{SUPABASE_URL}/rest/v1/doorloop_error_records?status=eq.unresolved&limit=10",
        headers=headers
    )
    records = resp.json()

    if not records:
        print("✅ No unresolved DLQ records found.")
        return

    for record in records:
        print(f"🔁 Retrying {record['entity_type']} {record['doorloop_id']}")

        # Step 2: Patch status to 'retried' and increment retry_count
        patch_payload = {
            "status": "retried",
            "retry_count": record.get("retry_count", 0) + 1
        }

        update_url = f"{SUPABASE_URL}/rest/v1/doorloop_error_records?id=eq.{record['id']}"
        patch_resp = requests.patch(update_url, headers=headers, data=json.dumps(patch_payload))

        if patch_resp.status_code == 204:
            print(f"✅ Updated DLQ record {record['id']}")
        else:
            print(f"❌ Failed to update record {record['id']}: {patch_resp.text}")

if __name__ == "__main__":
    retry_dlq_via_rest()
