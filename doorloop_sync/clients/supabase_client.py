import os
import logging
from supabase import create_client, Client
from postgrest.exceptions import APIError
from doorloop_sync.utils.utils_data import standardize_record

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set.")
        self.supabase: Client = create_client(url, key)

    def fetch_all(self, table: str):
        logger.info(f"ℹ️ Fetching all records from table: {table}")
        try:
            response = self.supabase.table(table).select("*").execute()
            return response.data
        except APIError as e:
            logger.error(f"❌ API Error fetching from '{table}': {e.message}")
            return []
        except Exception as e:
            logger.error(f"❌ Unexpected Error fetching from '{table}': {e}", exc_info=True)
            return []

    def upsert(self, table: str, data: list, on_conflict_column: str = None):
        if not data:
            logger.warning(f"⏭️ Skipping upsert to '{table}' — payload was empty.")
            return {}

        logger.info(f"ℹ️ Upserting {len(data)} records to table: {table}")
        try:
            standardized_data = [standardize_record(item) for item in data]
            response = self.supabase.table(table).upsert(
                standardized_data, 
                on_conflict=on_conflict_column
            ).execute()

            logger.info(f"✅ Successfully upserted {len(standardized_data)} records to '{table}'.")
            return response
        except APIError as e:
            logger.error(f"❌ Supabase APIError on upsert to '{table}' with {len(data)} records.")
            logger.error(f"   Error Message: {e.message}")
            if data:
                logger.error(f"   Sample Payload Keys: {list(data[0].keys())}")
            raise
        except Exception as e:
            logger.error(f"❌ Unexpected Error upserting to '{table}': {e}", exc_info=True)
            raise