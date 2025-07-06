import requests
import logging
import json
import dateutil.parser # For improved timestamp detection

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
        if not r.ok: # Check for non-2xx status codes
            logging.error(f"❌ Failed to execute SQL: {sql} → {r.status_code}: {r.text}")
            r.raise_for_status() # Raise an exception for HTTP errors
        else:
            logging.info(f"✅ SQL execution succeeded: {sql.strip().splitlines()[0]}...") # Log first line of SQL
            # Safely return JSON if available, otherwise empty dict
            try:
                return r.json()
            except json.JSONDecodeError:
                return {}


    def ensure_raw_table_exists(self, table_name):
        """
        Ensures that a raw table exists in Supabase.
        If it doesn't, it creates it with `id` (TEXT PRIMARY KEY),
        'data' (JSONB), 'source_endpoint' (TEXT), and 'inserted_at' (TIMESTAMP) columns.
        """
        logging.info(f"🛠️ Ensuring table exists: {table_name}")
        # The crucial change: `id TEXT PRIMARY KEY`
        create_sql = f"""
        CREATE TABLE IF NOT EXISTS public."{table_name}" (
            id TEXT PRIMARY KEY, -- THIS IS THE CRITICAL CHANGE FROM UUID TO TEXT
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
            raise # Re-raise if table creation fails, as it's critical

    def add_missing_columns(self, table_name, records):
        logging.info(f"📊 Scanning fields for table: {table_name}")
        existing_columns = self._get_existing_columns(table_name)
        logging.debug(f"🔎 Existing columns: {existing_columns}")
        proposed_columns = set()
        
        for record in records:
            for key, value in record.items():
                # Skip 'id', 'data', 'source_endpoint', 'inserted_at' as they are fixed columns
                if key in ['id', 'data', 'source_endpoint', 'inserted_at']:
                    continue
                
                if key not in existing_columns:
                    # Check for nested JSON, treat as JSONB
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
                # Log a warning instead of crashing if a column can't be added (e.g., due to type conflict if it exists)
                logging.warning(f"⚠️ Failed to add column '{col_name}' to '{table_name}': {e}. This might indicate a type mismatch if the column already exists with a different type.")

        # Force schema cache refresh
        ping_url = f"{self.supabase_url}/rest/v1/"
        try:
            ping = requests.get(ping_url, headers=self.headers)
            ping.raise_for_status() # Raise for HTTP errors
            logging.info(f"🔁 PostgREST schema refresh response: {ping.status_code}")
        except requests.exceptions.RequestException as e:
            logging.warning(f"⚠️ Failed to refresh PostgREST schema cache at {ping_url}: {e}")


    def _get_existing_columns(self, table_name):
        url = f"{self.supabase_url}/rest/v1/{table_name}?limit=1"
        try:
            r = requests.get(url, headers=self.headers)
            r.raise_for_status() # Raise an exception for HTTP errors
            
            data = r.json()
            if data and isinstance(data, list) and len(data) > 0:
                # Include the fixed columns as they will always be there if the table exists
                return list(data[0].keys())
            else:
                logging.info(f"🔍 No records found for {table_name} (table might be empty).")
                # If table is empty or newly created, it should have the base columns
                return ['id', 'data', 'source_endpoint', 'inserted_at']
        except requests.exceptions.RequestException as e:
            # This handles cases where the table literally does not exist yet (e.g., first run)
            # or permissions issues during column check (though SQL execute should catch 403 earlier).
            logging.warning(f"Could not retrieve existing columns for {table_name} (likely not yet created or no data): {e}")
            return ['id', 'data', 'source_endpoint', 'inserted_at']


    def _infer_sql_type(self, value):
        if isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "bigint"
        elif isinstance(value, float):
            return "double precision"
        elif isinstance(value, str):
            # Check for common date/time formats
            if len(value) >= 10 and (value.count('-') == 2 or 'T' in value or '+' in value or 'Z' in value):
                try:
                    dateutil.parser.isoparse(value)
                    return "timestamp with time zone"
                except (ValueError, AttributeError):
                    pass # Not a valid ISO timestamp, fall through
            
            # Check for mongoId-like strings (24 hex characters)
            if len(value) == 24 and all(c in '0123456789abcdefABCDEF' for c in value):
                return "text" # Treat mongoId as text
            
            return "text" # Default for strings
        elif isinstance(value, list) or isinstance(value, dict):
            return "jsonb" # For nested JSON structures
        elif value is None:
            return "text" # Nullable text as a fallback
        else:
            return "text" # Default for any other unexpected type