
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

    def _execute_sql(self, sql):
        payload = {"sql": sql}
        r = requests.post(f"{self.supabase_url}/rest/v1/rpc/execute_sql", headers=self.headers, json=payload)
        if r.status_code not in [200, 201, 204]:
            logging.error(f"❌ Failed to execute SQL: {sql} → {r.status_code}: {r.text}")
        else:
            logging.info(f"✅ Column patch succeeded: {sql}")

    def add_missing_columns(self, table_name, records):
        logging.info(f"📊 Scanning fields for table: {table_name}")
        existing_columns = self._get_existing_columns(table_name)
        logging.debug(f"🔎 Existing columns: {existing_columns}")
        proposed_columns = set()
        for record in records:
            logging.debug(f"🔍 Scanning record: {record}")
            for key, value in record.items():
                if key not in existing_columns:
                    proposed_columns.add((key, self._infer_sql_type(value)))

        for col_name, col_type in proposed_columns:
            sql = f'ALTER TABLE public."{table_name}" ADD COLUMN IF NOT EXISTS "{col_name}" {col_type};'
            logging.info(f"🛠️ Executing SQL: {sql}")
            self._execute_sql(sql)

        # Force schema cache refresh
        ping = requests.get(f"{self.supabase_url}/rest/v1/", headers=self.headers)
        logging.info(f"🔁 PostgREST schema refresh response: {ping.status_code}")

    def _get_existing_columns(self, table_name):
        url = f"{self.supabase_url}/rest/v1/{table_name}?limit=1"
        r = requests.get(url, headers=self.headers)
        if r.status_code == 200:
            return list(r.json()[0].keys()) if r.json() else []
        return []

    def _infer_sql_type(self, value):
        if isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "bigint"
        elif isinstance(value, float):
            return "float"
        elif isinstance(value, str):
            return "text"
        else:
            return "text"
