# doorloop_sync/clients/supabase_client.py

import os
import postgrest
import gotrue
from supabase import create_client

class SupabaseIngestClient:
    def __init__(self):
        url = os.environ["SUPABASE_URL"]
        key = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
        self.client = create_client(url, key)

    def upsert_records(self, table_name: str, records: list):
        if not records:
            print(f"[⚠️] No records to upsert for {table_name}")
            return
        response = self.client.table(table_name).upsert(records).execute()
        print(f"[📝] Upserted {len(records)} records into {table_name}")
        return response

    def fetch_raw(self, table_name: str):
        response = self.client.table(table_name).select("*").execute()
        return response.data or []
