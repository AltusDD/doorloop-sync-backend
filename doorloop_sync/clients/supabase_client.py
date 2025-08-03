from supabase import create_client, Client

class SupabaseClient:
    def __init__(self, url: str, key: str):
        self.supabase: Client = create_client(url, key)

    def upsert(self, table: str, data: list):
        response = self.supabase.table(table).upsert(data).execute()
        return response

    def fetch_all(self, table: str):
        response = self.supabase.table(table).select("*").execute()
        return response.data

# silent_update
