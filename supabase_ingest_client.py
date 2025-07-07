
import os
import requests
import logging
import time

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

HEADERS = {
    "apikey": SUPABASE_SERVICE_ROLE_KEY,
    "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
    "Content-Type": "application/json"
}

def upsert_data(table_name, records):
    if not records:
        logging.info(f"📭 No records to upsert for {table_name}")
        return

    if table_name == "doorloop_raw_communications":
        logging.info(f"📦 Using batch insert for {table_name} due to potential payload size")
        return batch_insert(table_name, records, batch_size=50)

    url = f"{SUPABASE_URL}/rest/v1/{table_name}?on_conflict=id"
    try:
        response = requests.post(url, headers=HEADERS, json=records, timeout=30)
        if response.status_code == 200:
            logging.info(f"✅ Upserted {len(records)} records into {table_name}")
        else:
            logging.error(f"❌ Supabase insert failed for {table_name}: {response.status_code} → {response.text}")
            response.raise_for_status()
    except Exception as e:
        logging.exception(f"🔥 Exception during upsert → {e}")
        raise

def batch_insert(table_name, records, batch_size=50):
    url = f"{SUPABASE_URL}/rest/v1/{table_name}?on_conflict=id"
    for i in range(0, len(records), batch_size):
        chunk = records[i:i+batch_size]
        try:
            response = requests.post(url, headers=HEADERS, json=chunk, timeout=30)
            if response.status_code == 200:
                logging.info(f"✅ Batch inserted {len(chunk)} records into {table_name}")
            else:
                logging.error(f"❌ Batch insert failed for {table_name}: {response.status_code} → {response.text}")
                response.raise_for_status()
        except Exception as e:
            logging.exception(f"🔥 Exception during batch insert → {e}")
            raise
