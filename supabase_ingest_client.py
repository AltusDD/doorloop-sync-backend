
import requests
import logging

logger = logging.getLogger(__name__)

class SupabaseIngestClient:
    def __init__(self, url: str, api_key: str):
        self.base_url = url.rstrip("/")
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

        # ✅ Normalize: Ensure all dicts have the same keys
        all_keys = set().union(*(record.keys() for record in records if isinstance(record, dict)))
        normalized_records = [
            {key: record.get(key, None) for key in all_keys}
            for record in records if isinstance(record, dict)
        ]

        url = f"{self.base_url}/{table}?on_conflict=id"
        try:
            response = requests.post(url, headers=self.headers, json=normalized_records)
            if response.status_code >= 400:
                logger.error(f"❌ Supabase insert failed for {table}: {response.status_code} → {response.text}")
                response.raise_for_status()
            logger.info(f"✅ Upsert successful for {table}")
        except requests.RequestException as e:
            logger.error(f"🔥 Exception during upsert → {e}")
            raise
