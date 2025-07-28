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

    def _request(self, method: str, url: str, data: Any = None, params: Dict[str, Any] = None, retries: int = 3):
        for attempt in range(1, retries + 1):
            try:
                resp = self.session.request(method, url, json=data, params=params, timeout=30)
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                logger.warning(f"Request error (attempt {attempt}): {e}")
                if attempt == retries:
                    raise
                time.sleep(2 ** attempt)

    def upsert_data(self, table: str, records: List[Dict[str, Any]], on_conflict: str = "id"):
        url = f"{self.supabase_url}/rest/v1/{table}"
        params = {"on_conflict": on_conflict, "returning": "representation"}
        logger.info(f"🔁 Upserting {len(records)} records to table '{table}'")
        return self._request("POST", url, data=records, params=params)

    def log_audit(self, batch_id: str, status: str, entity: str, message: str = "", record_count: Optional[int] = None):
        url = f"{self.supabase_url}/rest/v1/doorloop_pipeline_audit"
        record = {
            "batch_id": batch_id,
            "status": status,
            "entity": entity,
            "message": message,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        if record_count is not None:
            record["record_count"] = record_count
        logger.info(f"📝 Logging audit: {record}")
        self._request("POST", url, data=[record])

    def log_dlq(self, batch_id: str, entity: str, record: Dict[str, Any], error: str):
        url = f"{self.supabase_url}/rest/v1/doorloop_error_records"
        dlq_entry = {
            "batch_id": batch_id,
            "entity": entity,
            "record": record,
            "error": error,
            "timestamp": int(time.time())
        }
        logger.warning(f"⚠️ DLQ: {dlq_entry}")
        self._request("POST", url, data=[dlq_entry])
