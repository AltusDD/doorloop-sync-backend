import requests
import logging
import json
import dateutil.parser

logger = logging.getLogger(__name__)

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
        if not r.ok:
            logging.error(f"❌ Failed to execute SQL: {sql} → {r.status_code}: {r.text}")
            r.raise_for_status()
        else:
            logging.info(f"✅ SQL execution succeeded: {sql.strip().splitlines()[0]}...")
            try:
                return r.json()
            except json.JSONDecodeError:
                return {}

    def ensure_raw_table_exists(self, table_name):
        logging.info(f"🛠️ Ensuring table exists: {table_name}")
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS public."{table_name}" (
            id TEXT PRIMARY KEY,
            data JSONB,
            source_endpoint TEXT,
            inserted_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        );
        """
        try:
            self._execute_sql(create_sql)
            logging.info(f"✅ Table '{table_name}' ensured (created or already exists).")
        except Exception as e:
            logging.error(f"❌ Failed to ensure table '{table_name}': {e}")
            raise

    def add_missing_columns(self, table_name, records):
        logging.info(f"📊 Scanning fields for table: {table_name}")
        existing_columns = self._get_existing_columns(table_name)
        logging.debug(f"🔎 Existing columns: {existing_columns}")
        proposed_columns = set()

        for record in records:
            for key, value in record.items():
                if key in ['id', 'data', 'source_endpoint', 'inserted_at']:
                    continue
                if key not in existing_columns:
                    if isinstance(value, (dict, list)):
                        proposed_columns.add((key, "jsonb"))
                    else:
                        proposed_columns.add((key, self._infer_sql_type(value)))

        for col_name, col_type in proposed_columns:
            sql = f'ALTER TABLE public."{table_name}" ADD COLUMN IF NOT EXISTS "{col_name}" {col_type};'
            logging.info(f"🛠️ Executing SQL: {sql}")
            try:
                self._execute_sql(sql)
            except Exception as e:
                logging.warning(f"⚠️ Failed to add column '{col_name}' to '{table_name}': {e}")

        try:
            ping_url = f"{self.supabase_url}/rest/v1/"
            ping = requests.get(ping_url, headers=self.headers)
            ping.raise_for_status()
            logging.info(f"🔁 PostgREST schema refresh response: {ping.status_code}")
        except requests.exceptions.RequestException as e:
            logging.warning(f"⚠️ Failed to refresh PostgREST schema cache at {ping_url}: {e}")

    def _get_existing_columns(self, table_name):
        url = f"{self.supabase_url}/rest/v1/{table_name}?limit=1"
        try:
            r = requests.get(url, headers=self.headers)
            r.raise_for_status()
            data = r.json()
            if data and isinstance(data, list) and len(data) > 0:
                return list(data[0].keys())
            else:
                logging.info(f"🔍 No records found for {table_name} (table might be empty).")
                return ['id', 'data', 'source_endpoint', 'inserted_at']
        except requests.exceptions.RequestException as e:
            logging.warning(f"Could not retrieve existing columns for {table_name}: {e}")
            return ['id', 'data', 'source_endpoint', 'inserted_at']

    def _infer_sql_type(self, value):
        if isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "bigint"
        elif isinstance(value, float):
            # 🔧 Fix: if value has decimal portion → use NUMERIC
            if not value.is_integer():
                return "NUMERIC"
            return "bigint"
        elif isinstance(value, str):
            if len(value) >= 10 and (value.count('-') == 2 or 'T' in value or '+' in value or 'Z' in value):
                try:
                    dateutil.parser.isoparse(value)
                    return "timestamp with time zone"
                except (ValueError, AttributeError):
                    pass
            if len(value) == 24 and all(c in '0123456789abcdefABCDEF' for c in value):
                return "text"
            return "text"
        elif isinstance(value, list) or isinstance(value, dict):
            return "jsonb"
        elif value is None:
            return "text"
        else:
            return "text"

    def upsert_data(self, table_name, records):
        url = f"{self.supabase_url}/rest/v1/{table_name}?on_conflict=id"
        r = requests.post(url, headers=self.headers, json=records)
        if r.status_code == 409:
            logging.warning(f"⚠️ Supabase 409 Conflict for {table_name}: {r.text}")
            return
        if not r.ok:
            logging.error(f"❌ Supabase insert failed for {table_name}: {r.status_code} → {r.text}")
            r.raise_for_status()
        else:
            logging.info(f"✅ Supabase upsert succeeded for {table_name}")
