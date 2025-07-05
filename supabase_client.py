
import logging
import requests
import json

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self, url: str, service_role_key: str):
        self.url = url.rstrip("/")
        self.headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json"
        }

    def upsert_data(self, table: str, records: list):
        if not records:
            logger.info(f"⚠️ No records to upsert for table: {table}")
            return

        # 🔧 Normalize all records to have identical keys
        all_keys = set(k for record in records for k in record)
        records = [{k: record.get(k, None) for k in all_keys} for record in records]

        url = f"{self.url}/rest/v1/{table}?on_conflict=id"
        logger.debug(f"📤 Posting {len(records)} records to {url}")
        try:
            r = requests.post(url, headers=self.headers, data=json.dumps(records))
            r.raise_for_status()
            logger.info(f"✅ Supabase upsert succeeded for {table}")
        except requests.exceptions.HTTPError as e:
            logger.error(f"❌ Supabase insert failed for {table}: {r.status_code} → {r.text}")
            raise
