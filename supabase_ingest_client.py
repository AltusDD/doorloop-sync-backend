import requests
import logging
import time
from typing import List, Dict, Any, Optional

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class SupabaseIngestClient:
    def __init__(self, supabase_url: str, service_role_key: str):
        self.supabase_url = supabase_url.rstrip('/')
        self.service_role_key = service_role_key
        self.headers = {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
            "Content-Type": "application/json"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)
        logger.info("✅ SupabaseIngestClient initialized.")

    def _request(self, method: str, url: str, data: Optional[Any] = None, params: Optional[Dict[str, Any]] = None, retries: int = 3) -> requests.Response:
        for attempt in range(1, retries + 1):
            try:
                response = self.session.request(method, url, headers=self.headers, json=data, params=params, timeout=30)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                try:
                    error_detail = e.response.json()
                except Exception:
                    error_detail = e.response.text
                logger.warning(f"Request error (attempt {attempt}): {e} | Details: {error_detail}")
                if attempt == retries:
                    raise
                time.sleep(2 ** attempt)

    def upsert_data(self, table_name: str, records: List[Dict[str, Any]], on_conflict: str = "doorloop_id") -> requests.Response:
        logger.info(f"🔁 Upserting {len(records)} records into {table_name} (conflict key: {on_conflict})")
        url = f"{self.supabase_url}/rest/v1/{table_name}"
        params = {"on_conflict": on_conflict, "returning": "representation"}
        return self._request("POST", url, data=records, params=params)

    def log_audit(self, batch_id: str, status: str, entity: str, message: str = "", record_count: Optional[int] = None):
        url = f"{self.supabase_url}/rest/v1/doorloop_pipeline_audit"
        record = {
            "batch_id": batch_id,
            "status": status,
            "entity": entity,
            "message": message,
        }
        if record_count is not None:
            record["record_count"] = record_count
        logger.info(f"📝 Logging audit: {record}")
        self._request("POST", url, data=[record])

    def log_dlq(self, batch_id: str, entity: str, record: Dict[str, Any], error: str):
        url = f"{self.supabase_url}/rest/v1/doorloop_error_records"
        dlq = {
            "batch_id": batch_id,
            "entity": entity,
            "record": record,
            "error": error,
            "timestamp": int(time.time())
        }
        logger.warning(f"⚠️ Logging DLQ record: {dlq}")
        self._request("POST", url, data=[dlq])
