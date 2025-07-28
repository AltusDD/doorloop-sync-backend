import os
import logging
import requests
from typing import List, Dict, Any
from datetime import datetime

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SupabaseIngestClient:
    def __init__(self, supabase_url: str, service_role_key: str):
        self.supabase_url = supabase_url.rstrip('/')
        self.service_role_key = service_role_key
        self.headers = {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
            "Content-Type": "application/json",
            "Accept": "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        logger.info("✅ SupabaseIngestClient initialized.")

    def _request(self, method: str, url: str, data: Any = None, params: Dict[str, Any] = None) -> requests.Response:
        for attempt in range(3):
            try:
                if method.upper() == "POST":
                    response = self.session.post(url, json=data, params=params)
                elif method.upper() == "PATCH":
                    response = self.session.patch(url, json=data, params=params)
                elif method.upper() == "GET":
                    response = self.session.get(url, params=params)
                else:
                    raise ValueError(f"Unsupported HTTP method: {method}")
                
                response.raise_for_status()
                return response

            except requests.exceptions.RequestException as e:
                logger.warning(f"Request error (attempt {attempt + 1}): {e}")
                if attempt == 2:
                    raise

    def upsert_data(self, table_name: str, records: List[Dict[str, Any]]) -> None:
        url = f"{self.supabase_url}/rest/v1/{table_name}"
        params = {"on_conflict": "id"}  # Adjust conflict key as needed
        logger.info(f"📤 Upserting {len(records)} records to {table_name}...")
        self._request("POST", url, data=records, params=params)

    def log_audit(self, batch_id: str, status: str, entity: str, message: str) -> None:
        """Logs an audit record into the doorloop_pipeline_audit table."""
        url = f"{self.supabase_url}/rest/v1/doorloop_pipeline_audit"
        record = {
            "batch_id": batch_id,
            "status": status,
            "entity": entity,
            "message": message,
            "timestamp": datetime.utcnow().isoformat(),
            "entity_type": "sync"  # ✅ REQUIRED FOR SCHEMA CONSTRAINT
        }
        logger.info(f"📝 Logging audit: {record}")
        self._request("POST", url, data=[record])
