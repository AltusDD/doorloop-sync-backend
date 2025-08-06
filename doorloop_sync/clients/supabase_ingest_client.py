import os
import requests
from doorloop_sync.utils.logger import log_insert_success, log_insert_failure

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

class SupabaseIngestClient:
    def __init__(self):
        if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
            raise ValueError("Supabase credentials not set in environment variables.")
        self.base_url = f"{SUPABASE_URL}/rest/v1"
        self.headers = {
            "apikey": SUPABASE_SERVICE_ROLE_KEY,
            "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
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
