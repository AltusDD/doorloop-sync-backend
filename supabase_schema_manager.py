import requests
import logging

logger = logging.getLogger(__name__)

class SupabaseSchemaManager:
    def __init__(self, supabase_url, service_role_key):
        self.url = supabase_url.rstrip("/")
        self.headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json"
        }

    def _run_sql(self, sql):
        endpoint = f"{self.url}/rest/v1/rpc/execute_sql"
        response = requests.post(endpoint, headers=self.headers, json={"sql": sql})
        if response.status_code != 200:
            logger.error(f"❌ SQL failed: {sql}")
            logger.error(f"Response: {response.text}")
            return False
        return True

    def table_exists(self, table_name):
        sql = f"SELECT to_regclass('public.{table_name}')"
        endpoint = f"{self.url}/rest/v1/rpc/execute_sql"
        resp = requests.post(endpoint, headers=self.headers, json={"sql": sql})
        return table_name in resp.text

    def recreate_table(self, table_name, fields):
        logger.warning(f"⚠️ Recreating table: {table_name}")
        backup = f"{table_name}_backup"
        self._run_sql(f'ALTER TABLE "{table_name}" RENAME TO "{backup}";')
        columns = ", ".join([f'"{k}" {v}' for k, v in fields.items()])
        create_sql = f'CREATE TABLE "{table_name}" ({columns});'
        return self._run_sql(create_sql)

    def ensure_table_structure(self, table_name, sample_record):
        field_map = self.infer_types(sample_record)
        for column, sql_type in field_map.items():
            alter_sql = f'ALTER TABLE "{table_name}" ADD COLUMN IF NOT EXISTS "{column}" {sql_type};'
            success = self._run_sql(alter_sql)
            if not success and "must be owner" in alter_sql:
                return self.recreate_table(table_name, field_map)
        return True

    def infer_types(self, record):
        mapping = {}
        for k, v in record.items():
            if isinstance(v, str):
                mapping[k] = "text"
            elif isinstance(v, int):
                mapping[k] = "integer"
            elif isinstance(v, float):
                mapping[k] = "numeric"
            elif isinstance(v, bool):
                mapping[k] = "boolean"
            elif isinstance(v, dict) or isinstance(v, list):
                mapping[k] = "jsonb"
            elif v is None:
                mapping[k] = "text"
            else:
                mapping[k] = "text"
        return mapping
