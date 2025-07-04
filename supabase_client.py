
import requests
import logging
from supabase_schema_manager import ensure_table_structure

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class SupabaseClient:
    def __init__(self, supabase_url: str, supabase_key: str):
        self.url = supabase_url
        self.headers = {
            "apikey": supabase_key,
            "Authorization": f"Bearer {supabase_key}",
            "Content-Type": "application/json"
        }

    def ensure_table_and_columns(self, table_name: str, sample_record: dict):
        try:
            ensure_table_structure(self.url, self.headers, table_name, sample_record)
        except Exception as e:
            logger.error(f"❌ Schema enforcement failed for {table_name}: {e}")
            raise

    def upsert_data(self, table_name: str, data: list, conflict_key: str = "id"):
        if not data:
            logger.info(f"⚠️ No data to upsert for {table_name}")
            return

        url = f"{self.url}/rest/v1/{table_name}?on_conflict={conflict_key}"
        logger.debug(f"⬆️ Upserting {len(data)} records to {table_name}")

        response = requests.post(url, headers=self.headers, json=data)
        if not response.ok:
            logger.error(f"❌ Supabase insert failed for {table_name}: {response.status_code} {response.text}")
            response.raise_for_status()

        logger.info(f"✅ Upserted {len(data)} records into {table_name}")
