from supabase import create_client, Client
import logging

class SupabaseClient:
    def __init__(self, url: str, key: str):
        self.supabase: Client = create_client(url, key)

    def upsert(self, table: str, data: list):
        # FIX: Add a guard clause to prevent API errors when 'data' is an empty list.
        if not data:
            logging.info(f"Skipping upsert for table '{table}' because there is no data.")
            return None

        response = self.supabase.table(table).upsert(data).execute()
        return response

    def fetch_all(self, table: str):
        response = self.supabase.table(table).select("*").execute()
        return response.data