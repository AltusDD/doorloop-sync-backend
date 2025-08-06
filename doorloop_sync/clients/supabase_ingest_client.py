import os
import requests
from doorloop_sync.utils.logger import log_insert_success, log_insert_failure

class SupabaseIngestClient:
    def __init__(self):
        self.url = os.getenv("SUPABASE_URL")
        self.key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not self.url or not self.key:
            raise EnvironmentError("Supabase credentials not set in environment variables.")

        self.base_url = f"{self.url}/rest/v1"
        self.headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }

    def insert_records(self, table: str, records: list[dict], entity: str):
        if not records:
            return
        url = f"{self.base_url}/{table}"
        try:
            response = requests.post(url, json=records, headers=self.headers)
            if response.status_code in (200, 201):
                log_insert_success(entity, len(records))
            else:
                log_insert_failure(entity, response.status_code, response.text)
        except Exception as e:
            log_insert_failure(entity, "exception", str(e))
