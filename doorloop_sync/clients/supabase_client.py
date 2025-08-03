import os
import logging
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

class SupabaseClient:
    def __init__(self):
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        self.supabase: Client = create_client(self.url, self.key)
        logging.info("✅ SupabaseClient initialized.")

    def upsert(self, table: str, data: list[dict]):
        if not data:
            logging.warning(f"No data to upsert for table {table}")
            return
        try:
            response = self.supabase.table(table).upsert(data).execute()
            logging.info(f"✅ Upserted {len(data)} records into {table}")
            return response
        except Exception as e:
            logging.error(f"❌ Failed to upsert data to {table}: {e}")
            raise

    def insert(self, table: str, data: list[dict]):
        if not data:
            logging.warning(f"No data to insert for table {table}")
            return
        try:
            response = self.supabase.table(table).insert(data).execute()
            logging.info(f"✅ Inserted {len(data)} records into {table}")
            return response
        except Exception as e:
            logging.error(f"❌ Failed to insert data to {table}: {e}")
            raise

    def delete_all(self, table: str):
        try:
            response = self.supabase.table(table).delete().neq("id", "").execute()
            logging.info(f"🗑️ Deleted all records from {table}")
            return response
        except Exception as e:
            logging.error(f"❌ Failed to delete all from {table}: {e}")
            raise

    def fetch_all(self, table: str):
        try:
            response = self.supabase.table(table).select("*").execute()
            logging.info(f"📥 Fetched {len(response.data)} records from {table}")
            return response.data
        except Exception as e:
            logging.error(f"❌ Failed to fetch data from {table}: {e}")
            raise
