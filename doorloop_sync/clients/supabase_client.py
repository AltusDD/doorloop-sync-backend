import os
from supabase import create_client, Client

class SupabaseClient:
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        self.supabase: Client = create_client(url, key)

    def upsert(self, table, data):
        response = self.supabase.table(table).upsert(data).execute()
        return response