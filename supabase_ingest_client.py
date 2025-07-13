import json
import logging
import requests

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class SupabaseIngestClient:
    def __init__(self, url: str, api_key: str):
        self.url = url.rstrip("/")
        self.api_key = api_key
        self.headers = {
            "apikey": self.api_key,
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }

    def upsert_data(self, table: str, records: list):
        if not records:
            logger.warning(f"⚠️ No records to upsert for {table}")
            return

        endpoint = f"{self.url}/rest/v1/{table}"
        try:
            response = requests.post(
                endpoint,
                headers=self.headers,
                params={"on_conflict": "id"},
                data=json.dumps(records),
                timeout=60,
            )
            if response.status_code >= 400:
                logger.error(f"❌ Supabase insert failed for {table}: {response.status_code} → {response.text}")
                response.raise_for_status()
            else:
                logger.info(f"✅ Upsert successful for {table}")
        except Exception as e:
            logger.error(f"🔥 Exception during upsert → {e}")
            raise
