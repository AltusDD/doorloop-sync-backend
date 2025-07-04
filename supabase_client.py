import requests
import logging
from supabase_schema_manager import ensure_table_structure

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self, supabase_url: str, service_role_key: str):
        if not supabase_url or not service_role_key:
            raise ValueError("Supabase URL and Service Role Key must be provided.")
        self.supabase_url = supabase_url
        self.service_role_key = service_role_key
        self.headers = {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
            "Content-Type": "application/json"
        }

    def ensure_table_and_columns(self, table_name: str, records: list):
        if not records:
            logger.warning(f"⚠️ No data provided to check schema for {table_name}.")
            return
        try:
            ensure_table_structure(self.supabase_url, self.service_role_key, table_name, records)
        except Exception as e:
            logger.error(f"❌ ensure_table_structure failed for {table_name}: {str(e)}")
            raise

    def upsert_data(self, table_name: str, records: list, conflict_key="id"):
        if not records:
            logger.info(f"ℹ️ No data to upsert for {table_name}. Skipping.")
            return
        url = f"{self.supabase_url}/rest/v1/{table_name}?on_conflict={conflict_key}"
        response = requests.post(url, headers=self.headers, json=records)
        if not response.ok:
            logger.error(f"❌ Supabase insert failed for {table_name}: {response.status_code} {response.text}")
            response.raise_for_status()
        logger.info(f"✅ Upserted {len(records)} records to {table_name}")
