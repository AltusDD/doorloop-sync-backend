
class SupabaseIngestClient:
    def __init__(self, url, key):
        self.url = url
        self.key = key

    def fetch(self, table):
        print(f"Mock fetch from {table}")
        return []
