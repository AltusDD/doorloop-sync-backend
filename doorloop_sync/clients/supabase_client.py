import logging
import datetime
from supabase.client import Client, create_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SupabaseClient:
    def __init__(self, supabase_url: str, supabase_service_role_key: str):
        self.supabase: Client = create_client(supabase_url, supabase_service_role_key)
        logging.info("✅ SupabaseClient initialized.")

    def upsert_raw_data(self, table_name: str, records: list):
        if not records:
            logging.info(f"No records to upsert into {table_name}.")
            return
        
        try:
            # The de-duplication is handled in the main pipeline script.
            # This function now just handles the upsert.
            # We use on_conflict="id" to ensure this is an upsert operation.
            self.supabase.table(table_name).upsert(records, on_conflict="id").execute()
            logging.info(f"✅ Successfully upserted {len(records)} records to {table_name}.")
        except Exception as e:
            logging.error(f"❌ Failed to upsert data to {table_name}: {e}")
            raise

    def log_audit(self, batch_id: str, status: str, entity: str, message: str, entity_type: str = 'sync'):
        audit_record = {
            'batch_id': batch_id,
            'status': status,
            'entity': entity,
            'message': message,
            'entity_type': entity_type,
            'timestamp': datetime.datetime.now(datetime.timezone.utc).isoformat()
        }
        try:
            self.supabase.table('audit_logs').insert(audit_record).execute()
            logging.info(f"📝 Logging audit for entity: {entity}, status: {status}")
        except Exception as e:
            logging.error(f"❌ Failed to log audit record: {e} | Record: {audit_record}")