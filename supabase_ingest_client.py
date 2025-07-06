import requests
import logging
from typing import List, Dict

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self, supabase_url: str, service_role_key: str):
        self.supabase_url = supabase_url.rstrip("/")
        self.headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json"
        }

    def upsert_data(self, table_name: str, records: List[Dict]):
        if not records:
            logger.warning(f"⚠️ No records to upsert for {table_name}")
            return

        url = f"{self.supabase_url}/rest/v1/{table_name}?on_conflict=id"
        try:
            response = requests.post(url, json=records, headers=self.headers)
            if response.status_code == 409:
                logger.warning(f"⚠️ Supabase 409 Conflict for {table_name}: {response.text}")
            elif response.status_code == 400:
                logger.error(f"❌ Supabase insert failed for {table_name}: {response.status_code} → {response.text}")
                response.raise_for_status()
            elif not response.ok:
                logger.error(f"❌ Supabase insert failed for {table_name}: {response.status_code} → {response.text}")
                response.raise_for_status()
            else:
                logger.info(f"✅ Supabase upsert succeeded for {table_name}")
        except requests.RequestException as e:
            logger.exception(f"🔥 Failed to sync {table_name} → {e}")
            raise
