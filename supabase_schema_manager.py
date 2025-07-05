
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

    def _get_existing_columns(self, table_name):
        url = f"{self.supabase_url}/information_schema.columns"
        params = {
            "table_name": f"eq.{table_name}",
            "select": "column_name"
        }
        resp = requests.get(url, headers=self.headers, params=params)
        if resp.status_code != 200:
            logging.warning(f"⚠️ Could not fetch columns for {table_name}: {resp.text}")
            return []
        return [col["column_name"] for col in resp.json()]

    def add_missing_columns(self, table_name, records):
        if not records:
            return

        existing_columns = self._get_existing_columns(table_name)
        proposed_columns = set()
        for record in records:
            for k, v in record.items():
                if k not in existing_columns:
                    proposed_columns.add((k, v))

        for column_name, sample_value in proposed_columns:
            col_type = self._infer_postgres_type(sample_value)
            if not col_type:
                continue
            sql = f'ALTER TABLE public."{table_name}" ADD COLUMN IF NOT EXISTS "{column_name}" {col_type};'
            logging.info(f"🛠️ Adding column: {column_name} → {col_type}")
            self._execute_sql(sql)

    def _infer_postgres_type(self, value):
        if isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "bigint"
        elif isinstance(value, float):
            return "double precision"
        elif isinstance(value, str):
            return "text"
        elif value is None:
            return "text"
        return None

    def _execute_sql(self, sql):
        url = f"{self.supabase_url}/rest/v1/rpc/execute_sql"
        payload = { "sql": sql }
        resp = requests.post(url, headers=self.headers, json=payload)
        if resp.status_code != 200:
            logging.warning(f"⚠️ Failed to execute SQL: {sql} → {resp.text}")
