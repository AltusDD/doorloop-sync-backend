
import os
import requests
import logging

class SupabaseClient:
    def __init__(self):
        self.url = os.environ.get("SUPABASE_URL")
        self.service_role_key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")
        if not self.url or not self.service_role_key:
            raise ValueError("Supabase URL or service role key is not set in environment variables.")
        self.headers = {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
            "Content-Type": "application/json",
            "Prefer": "return=representation"
        }

    def upsert(self, table_name, records, on_conflict=None):
        if not records:
            return

        endpoint = f"{self.url}/rest/v1/{table_name}"
        params = {}
        if on_conflict:
            params["on_conflict"] = on_conflict
            params["columns"] = ",".join(records[0].keys())

        response = requests.post(endpoint, headers=self.headers, json=records, params=params)
        if response.status_code >= 400:
            logging.error(f"Supabase upsert failed: {response.status_code} - {response.text}")
            raise Exception(f"Failed to upsert into {table_name}")
        logging.info(f"✅ Successfully upserted {len(records)} records to {table_name}.")

    def insert(self, table_name, records):
        if not records:
            return

        endpoint = f"{self.url}/rest/v1/{table_name}"
        response = requests.post(endpoint, headers=self.headers, json=records)
        if response.status_code >= 400:
            logging.error(f"Supabase insert failed: {response.status_code} - {response.text}")
            raise Exception(f"Failed to insert into {table_name}")
        logging.info(f"✅ Successfully inserted {len(records)} records to {table_name}.")
