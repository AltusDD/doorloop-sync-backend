
import requests
import logging

class SupabaseSchemaManager:
    def __init__(self, supabase_url, service_role_key):
        self.supabase_url = supabase_url
        self.headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json"
        }

    def _get_table_columns_from_db(self, table_name: str):
        url = f"{self.supabase_url}/information_schema.columns"
        params = {
            "table_name": f"eq.{table_name}",
            "select": "column_name"
        }
        resp = requests.get(url, headers=self.headers, params=params)
        if resp.status_code != 200:
            logging.error(f"Failed to fetch schema for table {table_name}: {resp.text}")
            return []
        return [col['column_name'] for col in resp.json()]
