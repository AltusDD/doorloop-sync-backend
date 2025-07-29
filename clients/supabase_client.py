import requests
import logging
import time
from typing import List, Dict, Any, Optional

class SupabaseClient:
    def __init__(self, supabase_url: str, service_role_key: str):
        self.supabase_url = supabase_url.rstrip('/')
        self.service_role_key = service_role_key
        self.headers = {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
            "Content-Type": "application/json"
        }

    def _call_supabase_rest(self, method: str, table_name: str, data: Optional[Any] = None, params: Optional[Dict[str, Any]] = None, retries: int = 3) -> requests.Response:
        url = f"{self.supabase_url}/rest/v1/{table_name}"
        for attempt in range(retries):
            try:
                resp = requests.request(method, url, headers=self.headers, json=data, params=params, timeout=30)
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                logging.error(f"Supabase REST call failed (attempt {attempt+1}/{retries}): {e}")
                if attempt == retries - 1:
                    raise
                time.sleep(2 ** attempt)

    def fetch_raw_data(self, table_name: str, limit: int = 1000, offset: int = 0) -> List[Dict[str, Any]]:
        params = {"limit": limit, "offset": offset}
        resp = self._call_supabase_rest("GET", table_name, params=params)
        return resp.json()

    def upsert_data(self, table_name: str, records: List[Dict[str, Any]], on_conflict: str = "id") -> requests.Response:
        params = {"on_conflict": on_conflict, "returning": "representation"}
        return self._call_supabase_rest("POST", table_name, data=records, params=params)

    def log_audit(self, batch_id: str, status: str, entity: str, message: str = ""):
        audit_record = {
            "batch_id": batch_id,
            "status": status,
            "entity": entity,
            "message": message,
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        }
        try:
            response = self._call_supabase_rest("POST", "doorloop_pipeline_audit", data=[audit_record])
            return response
        except Exception as e:
            logging.error(f"Failed to log audit record: {e}")
            return None

    def log_dlq(self, batch_id: str, entity: str, record: Dict[str, Any], error: str):
        dlq_record = {
            "batch_id": batch_id,
        try:
            self._call_supabase_rest("POST", "doorloop_error_records", data=[dlq_record])
        except Exception as e:
            logging.error(f"Failed to log DLQ record for batch_id={batch_id}, entity={entity}: {e}")
            "record": record,
            "error": error,
            "timestamp": int(time.time())
        }
        self._call_supabase_rest("POST", "doorloop_error_records", data=[dlq_record])