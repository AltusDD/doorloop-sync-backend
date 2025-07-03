import requests
import os
import logging
from datetime import datetime

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self, url: str, service_role_key: str):
        if not url:
            raise ValueError("SUPABASE_URL must be provided to SupabaseClient.")
        if not service_role_key:
            raise ValueError("SUPABASE_SERVICE_ROLE_KEY must be provided to SupabaseClient.")

        self.url = url
        self.service_role_key = service_role_key
        self.headers = {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }

    def to_snake_case(self, name):
        return ''.join(['_' + c.lower() if c.isupper() else c for c in name]).lstrip('_')

    def upsert_data(self, table_name: str, records: list, primary_key_field: str = "id"):
        if not records:
            logger.info(f"No records for {table_name}")
            return

        transformed = []
        for record in records:
            item = {}
            for k, v in record.items():
                key = self.to_snake_case(k)
                if key == "class":
                    key = "class_name"
                if isinstance(v, datetime):
                    item[key] = v.isoformat()
                elif isinstance(v, (list, dict)):
                    item[key] = v
                else:
                    item[key] = v
            transformed.append(item)

        url = f"{self.url}/rest/v1/{table_name}?on_conflict={primary_key_field}"
        try:
            r = requests.post(url, headers=self.headers, json=transformed)
            r.raise_for_status()
            logger.info(f"Successfully upserted {len(transformed)} records into {table_name}.")
        except requests.exceptions.RequestException as e:
            logger.error(f"Insert failed for {table_name}: {e}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Response: {e.response.text}")
            raise