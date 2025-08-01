
from supabase import create_client

class SupabaseIngestClient:
    def __init__(self, url, key):
        self.client = create_client(url, key)

    def fetch(self, table):
        response = self.client.table(table).select("*").execute()
        return response.data


    def upsert(self, table, data):
        if not data:
            print(f"⚠️ No data to upsert into {table}")
            return
        self.client.table(table).upsert(data).execute()
        print(f"✅ Upserted {len(data)} records to {table}")


    def rpc(self, fn, params):
        return self.client.rpc(fn, params).execute()
