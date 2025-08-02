# doorloop_sync/clients/supabase_client.py

import logging
from supabase import create_client, Client
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SupabaseIngestClient:
    def __init__(self):
        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
        self.client: Client = create_client(url, key)
        logging.info("✅ SupabaseIngestClient initialized.")

    def upsert_records(self, table_name: str, records: list, conflict_key: str = "doorloop_id"):
        if not records:
            logging.info(f"[⚠️] No records to upsert into {table_name}.")
            return

        chunk_size = 1000
        for i in range(0, len(records), chunk_size):
            chunk = records[i:i + chunk_size]
            try:
                self.client.table(table_name).upsert(chunk, on_conflict=[conflict_key]).execute()
                logging.info(f"[📝] Upserted {len(chunk)} records into {table_name}.")
            except Exception as e:
                logging.error(f"❌ Failed upserting into {table_name}: {e}")
                raise

    def fetch_records(self, table_name: str):
        try:
            return self.client.table(table_name).select("*").execute().data or []
        except Exception as e:
            logging.error(f"❌ Failed fetching from {table_name}: {e}")
            raise
