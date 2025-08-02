import logging
from supabase.client import Client, create_client

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class SupabaseIngestClient:
    def __init__(self, supabase_url: str, supabase_service_role_key: str):
        self.supabase: Client = create_client(supabase_url, supabase_service_role_key)
        logging.info("✅ SupabaseIngestClient initialized.")

    def upsert_raw_data(self, table_name: str, records: list):
        if not records:
            logging.info(f"No records to upsert into {table_name}.")
            return

        try:
            # Assuming 'id' is the primary key for upserting raw data
            # Use a chunking approach for large datasets
            chunk_size = 1000  # Adjust based on Supabase limits and record size
            for i in range(0, len(records), chunk_size):
                chunk = records[i:i + chunk_size]
                self.supabase.table(table_name).insert(chunk, upsert=True).execute()
                logging.info(f"  - Upserted chunk of {len(chunk)} records to {table_name}.")
            
            logging.info(f"✅ Successfully upserted {len(records)} records to {table_name}.")
        except Exception as e:
            logging.error(f"❌ Failed to upsert data to {table_name}: {e}")
            raise

    def log_audit(self, batch_id: str, status: str, entity: str, message: str, timestamp: str, entity_type: str = 'sync'):
        # Correctly accept 'timestamp' argument 
        audit_record = {
            'batch_id': batch_id,
            'status': status,
            'entity': entity,
            'message': message,
            'timestamp': timestamp,  # Now correctly assigned
            'entity_type': entity_type,
        }
        try:
            # Assuming 'audit_logs' is the table for your audit records
            self.supabase.table('audit_logs').insert(audit_record).execute()
            logging.info(f"📝 Logging audit: {audit_record}")
        except Exception as e:
            logging.error(f"❌ Failed to log audit record: {e} | Record: {audit_record}")
            # Consider re-raising if audit log failure is critical for your workflow
            # raise
