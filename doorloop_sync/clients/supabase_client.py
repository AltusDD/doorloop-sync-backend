import os
import logging
from supabase import create_client, Client

logger = logging.getLogger(__name__)

class SupabaseIngestClient:
    def __init__(self):
        supabase_url = os.getenv("SUPABASE_URL")
        supabase_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not supabase_url or not supabase_key:
            raise ValueError("Missing Supabase credentials in environment variables.")

        self.client: Client = create_client(supabase_url, supabase_key)
        logger.info("✅ SupabaseIngestClient initialized.")

    def upsert(self, table_name: str, records: list, match_columns: list = ["id"]):
        logger.debug(f"📤 Upserting {len(records)} records into '{table_name}'...")
        response = self.client.table(table_name).upsert(records, on_conflict=",".join(match_columns)).execute()
        logger.debug(f"✅ Upsert response: {response}")
        return response

    def insert(self, table_name: str, records: list):
        logger.debug(f"📤 Inserting {len(records)} records into '{table_name}'...")
        response = self.client.table(table_name).insert(records).execute()
        logger.debug(f"✅ Insert response: {response}")
        return response
