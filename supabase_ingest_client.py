import requests
import logging
import os

logger = logging.getLogger(__name__)
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_API_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

class SupabaseIngestClient:
    def __init__(self, url: str, api_key: str):
        if not url or not api_key:
            raise ValueError("Both Supabase URL and API key are required.")
        self.base_url = f"{url}/rest/v1"
        self.headers = {
            "apikey": api_key,
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }

    def upsert_data(self, table: str, records: list):
        if not records:
            logger.warning(f"⚠️ No records to upsert for {table}")
            return

        url = f"{self.base_url}/{table}?on_conflict=id"
        try:
            response = requests.post(url, headers=self.headers, json=records)
            if response.status_code >= 400:
                logger.error(f"❌ Supabase insert failed for {table}: {response.status_code} → {response.text}")
                response.raise_for_status()
            logger.info(f"✅ Upsert successful for {table}")
        except requests.RequestException as e:
            logger.error(f"🔥 Exception during upsert → {e}")
            raise

    def batch_insert(self, table: str, records: list, batch_size: int = 100):
        if not records:
            logger.warning(f"⚠️ No records to batch insert for {table}")
            return

        logger.info(f"📦 Using batch insert for {table} due to potential payload size")
        url = f"{self.base_url}/{table}?on_conflict=id"
        for i in range(0, len(records), batch_size):
            batch = records[i:i + batch_size]
            try:
                response = requests.post(url, headers=self.headers, json=batch)
                if response.status_code >= 400:
                    logger.error(f"❌ Batch insert failed for {table}: {response.status_code} → {response.text}")
                    response.raise_for_status()
            except requests.RequestException as e:
                logger.error(f"🔥 Exception during batch insert → {e}")
                raise
