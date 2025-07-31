
from supabase import create_client

class SupabaseClient:
    def __init__(self, url, key):
        self.client = create_client(url, key)

    def fetch(self, table):
        response = self.client.table(table).select("*").execute()
        return response.data
