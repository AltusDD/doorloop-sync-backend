
import requests
import logging

logger = logging.getLogger(__name__)

class SupabaseIngestClient:
    def __init__(self, url: str, api_key: str):
        self.url = url
        self.api_key = api_key
        self.headers = {
            "apikey": self.api_key,
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }

    def _normalize_records(self, records):
        if not records:
            return records
        # Get superset of keys
        all_keys = set()
        for rec in records:
            all_keys.update(rec.keys())
        # Fill missing keys with None
        normalized = []
        for rec in records:
            normalized.append({k: rec.get(k, None) for k in all_keys})
        return normalized

    def upsert_data(self, table_name, records):
        records = self._normalize_records(records)
        try:
            url = f"{self.url}/rest/v1/{table_name}?on_conflict=id"
            response = requests.post(url, json=records, headers=self.headers)
            if not response.ok:
                logger.error(f"❌ Supabase insert failed for {table_name}: {response.status_code} → {response.text}")
            response.raise_for_status()
            logger.info(f"✅ Upsert successful for {table_name}")
        except Exception as e:
            logger.error(f"🔥 Exception during upsert → {e}")
            raise

    def batch_insert(self, table_name, records, batch_size=500):
        records = self._normalize_records(records)
        for i in range(0, len(records), batch_size):
            batch = records[i:i+batch_size]
            self.upsert_data(table_name, batch)
