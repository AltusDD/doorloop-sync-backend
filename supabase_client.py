import requests
import logging
import json

def upsert_raw_doorloop_data(endpoint, records, supabase_url, supabase_key):
    """
    Upserts raw DoorLoop records into Supabase under a table name like 'doorloop_raw_properties'.
    Expects each record to contain at least an 'id' field from DoorLoop (used as 'doorloop_id').
    """
    table_name = f"doorloop_raw_{endpoint.strip('/').replace('-', '_')}"
    logging.info(f"📡 Target Supabase table: {table_name}")
    logging.info(f"📦 Records count: {len(records)}")

    headers = {
        "apikey": supabase_key,
        "Authorization": f"Bearer {supabase_key}",
        "Content-Type": "application/json",
        "Prefer": "resolution=merge-duplicates"
    }

    for idx, record in enumerate(records, start=1):
        try:
            doorloop_id = record.get("id")
            if not doorloop_id:
                logging.warning(f"⚠️ Skipping record {idx}: missing 'id' field")
                continue

            payload = {
                "doorloop_id": doorloop_id,
                "name": record.get("name"),
                "_raw_payload": record
            }

            url = f"{supabase_url}/rest/v1/{table_name}?on_conflict=doorloop_id"
            response = requests.post(url, headers=headers, data=json.dumps(payload))

            if response.status_code in [200, 201]:
                logging.info(f"✅ Record {idx}: Inserted/Upserted successfully.")
            else:
                error_msg = response.json() if response.content else response.text
                logging.error(f"❌ Record {idx}: Failed with status {response.status_code}\n   → Error: {error_msg}")

        except Exception as e:
            logging.error(f"🔥 Record {idx}: Exception during upsert → {str(e)}")
