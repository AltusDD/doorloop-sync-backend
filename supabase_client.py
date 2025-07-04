
import requests
import logging
from supabase_schema_manager import SupabaseSchemaManager

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self, supabase_url, service_role_key):
        self.url = supabase_url.rstrip("/")
        self.headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json"
        }
        self.schema_manager = SupabaseSchemaManager(supabase_url, service_role_key)

    def insert_records(self, table, records):
        if not records:
            logger.warning(f"⚠️ No records to insert for {table}")
            return

        sample = records[0]

        # 🛠️ Ensure table exists
        if not self.schema_manager.table_exists(table):
            logger.info(f"🧱 Creating missing table: {table}")
            self.schema_manager.create_table(table, sample)

        # 🔍 Add missing columns
        existing_cols = self.schema_manager.get_table_columns(table)
        self.schema_manager.add_missing_columns(table, sample, existing_cols)

        # ✅ Clean records to only include known fields
        cleaned = [{k: v for k, v in r.items() if k in existing_cols or k == "id"} for r in records]

        try:
            r = requests.post(
                f"{self.url}/rest/v1/{table}?on_conflict=id",
                headers=self.headers,
                json=cleaned
            )
            r.raise_for_status()
            logger.info(f"✅ Synced {len(cleaned)} records to {table}")
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Supabase insert failed for {table}: {e}")
            logger.error(f"Response: {r.text}")
