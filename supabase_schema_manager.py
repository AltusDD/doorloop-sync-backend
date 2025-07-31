import requests
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class SupabaseSchemaManager:
    def __init__(self, supabase_url: str, service_role_key: str):
        self.supabase_url = supabase_url.rstrip("/")
        self.service_role_key = service_role_key
        self.headers = {
            "apikey": self.service_role_key,
            "Authorization": f"Bearer {self.service_role_key}",
            "Content-Type": "application/json"
        }

    def _rest_url(self, table: str) -> str:
        return f"{self.supabase_url}/rest/v1/{table}"

    def _rpc_url(self, fn_name: str) -> str:
        return f"{self.supabase_url}/rest/v1/rpc/{fn_name}"

    def ensure_raw_table_exists(self, table_name: str) -> None:
        # Placeholder: Supabase doesn't currently support DDL via REST,
        # so assume tables are pre-created or use SQL directly if needed.
        logger.info(f"🔧 Verifying existence of table: {table_name} (requires SQL for true validation)")
        # Real DDL creation would be done outside this script using Supabase SQL editor or CLI migration.

    def get_existing_columns(self, table_name: str) -> List[str]:
        url = self._rest_url(table_name)
        try:
            resp = requests.options(url, headers=self.headers)
            resp.raise_for_status()
            return list(resp.json()["columns"].keys())
        except Exception:
            # OPTIONS may not work; fallback to select * and parse keys
            try:
                resp = requests.get(url, headers=self.headers, params={"limit": 1})
                resp.raise_for_status()
                rows = resp.json()
                return list(rows[0].keys()) if rows else []
            except Exception as e:
                logger.warning(f"Could not retrieve existing columns for {table_name}: {e}")
                return []

    def add_missing_columns(self, table_name: str, records: List[Dict[str, Any]]) -> None:
        if not records:
            logger.warning(f"⏭ No records provided to infer schema for {table_name}")
            return

        existing_columns = self.get_existing_columns(table_name)
        all_keys = set()
        for record in records:
            all_keys.update(record.keys())

        missing_columns = list(set(all_keys) - set(existing_columns))

        if not missing_columns:
            logger.info(f"✅ No missing columns detected in {table_name}")
            return

        logger.info(f"➕ Adding {len(missing_columns)} missing columns to {table_name}: {missing_columns}")

        for col in missing_columns:
            col_type = self._infer_postgres_type(records, col)
            alter_stmt = f'ALTER TABLE public.{table_name} ADD COLUMN IF NOT EXISTS "{col}" {col_type};'
            self._execute_sql(alter_stmt)

    def _infer_postgres_type(self, records: List[Dict[str, Any]], column_name: str) -> str:
        # Basic type inference
        for record in records:
            value = record.get(column_name)
            if isinstance(value, bool):
                return "BOOLEAN"
            elif isinstance(value, int):
                return "BIGINT"
            elif isinstance(value, float):
                return "DOUBLE PRECISION"
            elif isinstance(value, dict):
                return "JSONB"
            elif value is None:
                continue
            else:
                return "TEXT"
        return "TEXT"

    def _execute_sql(self, sql: str) -> None:
        url = self._rpc_url("execute_sql")
        payload = {"sql": sql}
        try:
            resp = requests.post(url, headers=self.headers, json=payload)
            resp.raise_for_status()
            logger.info(f"✅ Executed SQL: {sql}")
        except Exception as e:
            logger.error(f"❌ Failed to execute SQL: {sql} — {e}")
