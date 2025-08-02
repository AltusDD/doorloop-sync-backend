import logging
import datetime
from supabase.client import Client, create_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- DEFINITIVE FIX ---
# The class is now correctly named SupabaseClient to match what the rest of
# your application is trying to import.
class SupabaseClient:
    def __init__(self, supabase_url: str, supabase_service_role_key: str):
        self.supabase: Client = create_client(supabase_url, supabase_service_role_key)
        logging.info("✅ SupabaseClient initialized.")

    def upsert(self, table: str, data: list):
        """
        Upserts a list of records into a specified Supabase table.
        """
        if not data:
            logging.info(f"No records to upsert into {table}.")
            return
        
        try:
            # The supabase-py library's upsert handles on_conflict automatically
            # when a primary key is defined on the table.
            self.supabase.table(table).upsert(data).execute()
            logging.info(f"✅ Successfully upserted {len(data)} records to {table}.")
        except Exception as e:
            logging.error(f"❌ Failed to upsert data to {table}: {e}")
            raise

    def log_audit(self, message: str, entity: str, status: str = "info"):
        """
        Logs an audit trail message to the audit_logs table.
        """
        audit_record = {
            'entity': entity,
            'status': status,
            'message': message,
            'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        try:
            self.supabase.table('audit_logs').insert(audit_record).execute()
        except Exception as e:
            # Log to console if audit logging fails, but don't crash the pipeline
            logging.error(f"❌ Failed to log audit record: {e} | Record: {audit_record}")
