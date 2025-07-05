
import requests
import logging

class SupabaseClient:
    def __init__(self, url, service_role_key):
        self.base_url = url
        self.headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json"
        }

    def upsert_data(self, table_name, records):
        url = f"{self.base_url}/rest/v1/{table_name}?on_conflict=id"
        logging.debug(f"📤 Posting {len(records)} records to {url}")
        try:
            r = requests.post(url, headers=self.headers, json=records)
            if r.status_code >= 400:
                logging.error(f"❌ Supabase insert failed for {table_name}: {r.status_code} → {r.text}")
            r.raise_for_status()
            logging.info(f"✅ Upserted {len(records)} into {table_name}")
        except Exception as e:
            logging.exception(f"🔥 Exception during upsert to {table_name}: {e}")
            raise
