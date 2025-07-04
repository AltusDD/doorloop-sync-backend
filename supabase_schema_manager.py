
import requests
import logging
import json

logger = logging.getLogger(__name__)

class SupabaseSchemaManager:
    def __init__(self, url: str, service_role_key: str):
        self.url = url.rstrip("/")
        self.headers = {
            "apikey": service_role_key,
            "Authorization": f"Bearer {service_role_key}",
            "Content-Type": "application/json"
        }

    def table_exists(self, table_name):
        r = requests.get(
            f"{self.url}/rest/v1/{table_name}",
            headers=self.headers,
            params={"limit": 1}
        )
        return r.status_code == 200

    def get_table_columns(self, table_name):
        r = requests.get(
            f"{self.url}/rest/v1/{table_name}?limit=1",
            headers=self.headers
        )
        if r.status_code == 200 and r.json():
            return set(r.json()[0].keys())
        return set()

    def create_table(self, table_name, sample_record):
        columns = []
        for key, value in sample_record.items():
            if key == "id":
                continue  # id will be created manually
            col_type = self.infer_type(value)
            columns.append(f'"{key}" {col_type}')
        ddl = f'CREATE TABLE IF NOT EXISTS "{table_name}" (id uuid primary key, {", ".join(columns)});'
        return self.run_sql(ddl)

    def add_missing_columns(self, table_name, record, existing_cols):
        alter_stmts = []
        for key, value in record.items():
            if key not in existing_cols:
                col_type = self.infer_type(value)
                alter_stmts.append(f'ALTER TABLE "{table_name}" ADD COLUMN "{key}" {col_type};')

        for stmt in alter_stmts:
            self.run_sql(stmt)

    def infer_type(self, value):
        if isinstance(value, str):
            return "text"
        elif isinstance(value, int):
            return "integer"
        elif isinstance(value, float):
            return "numeric"
        elif isinstance(value, bool):
            return "boolean"
        else:
            return "jsonb"

    def run_sql(self, sql):
        payload = {"sql": sql}
        r = requests.post(
            f"{self.url}/rest/v1/rpc/execute_sql",
            headers=self.headers,
            data=json.dumps(payload)
        )
        if r.status_code != 200:
            logger.error(f"❌ SQL failed: {sql}")
            logger.error(f"Response: {r.text}")
        return r.ok
