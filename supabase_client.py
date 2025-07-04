import requests
import logging
import time
from supabase_schema_manager import SupabaseSchemaManager

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self, url: str, service_role_key: str):
        self.url = url.rstrip("/")
        self.key = service_role_key
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json"
        }
        self.schema_manager = SupabaseSchemaManager(self.url, self.key)

    def _sanitize(self, table: str, records: list[dict]) -> list[dict]:
        response = requests.get(f"{self.url}/rest/v1/{table}?limit=1", headers=self.headers)
        if response.status_code != 200:
            logger.warning(f"⚠️ Could not fetch sample record for table {table}. Proceeding unsanitized.")
            return records
        existing_keys = set(response.json()[0].keys()) if response.json() else set()
        sanitized = []
        for record in records:
            clean = {k: v for k, v in record.items() if k in existing_keys}
            sanitized.append(clean)
        return sanitized

    def upsert(self, table: str, records: list[dict], pk_field: str = "id"):
        if not records:
            logger.info(f"⚠️ No records to upsert for table: {table}")
            return

        logger.debug(f"🔄 Attempting upsert of {len(records)} records into {table}")
        try:
            response = requests.post(
                f"{self.url}/rest/v1/{table}?on_conflict={pk_field}",
                headers=self.headers,
                json=records
            )
            if response.status_code in [200, 201]:
                logger.info(f"✅ Upserted {len(records)} records into {table}")
            elif response.status_code == 400 and "All object keys must match" in response.text:
                logger.warning(f"🛠️ Schema mismatch on {table}. Attempting auto-repair...")
                self._repair_schema(table, records)
                clean_records = self._sanitize(table, records)
                self.upsert(table, clean_records)
            else:
                logger.error(f"❌ Supabase insert failed for {table}: {response.status_code} {response.text}")
        except Exception as e:
            logger.error(f"🔥 Exception during upsert to {table}: {e}")

    def _repair_schema(self, table: str, records: list[dict]):
        try:
            first_record = records[0]
            logger.debug(f"🔧 Repairing schema for table: {table}")
            self.schema_manager.ensure_table_structure(table, first_record)
        except Exception as e:
            logger.error(f"🔥 Error during schema repair for {table}: {e}")
