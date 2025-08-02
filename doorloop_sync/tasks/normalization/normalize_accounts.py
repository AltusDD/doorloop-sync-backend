import logging
# --- CORRECTED IMPORT PATTERN ---
# All tasks get their dependencies from the centralized config module.
from doorloop_sync.config import get_supabase_client, get_logger

logger = get_logger(__name__)

def run():
    """
    Normalizes raw account data and upserts it into the normalized table.
    """
    logger.info("Starting normalization for accounts...")
    
    try:
        # Get an initialized Supabase client from the config
        supabase_client = get_supabase_client()
        
        raw_table_name = "doorloop_raw_accounts"
        normalized_table_name = "doorloop_normalized_accounts"

        # 1. Fetch raw data from Supabase
        # We select the 'data' column which contains the original JSON payload.
        response = supabase_client.supabase.table(raw_table_name).select("data").execute()
        raw_data = response.data
        
        if not raw_data:
            logger.info("No raw account data to normalize. Task complete.")
            return

        # 2. Normalize data (example transformation)
        normalized_records = []
        for row in raw_data:
            record = row['data'] # Extract the JSON payload
            normalized_records.append({
                "doorloop_id": record.get("id"),
                "name": record.get("name"),
                "type": record.get("type"),
                "balance": record.get("balance"),
                # Add other fields as needed, performing type casting and cleaning
            })

        # 3. Upsert normalized data
        if normalized_records:
            supabase_client.upsert(table=normalized_table_name, data=normalized_records)
            logger.info(f"Successfully normalized and upserted {len(normalized_records)} account records.")

    except Exception as e:
        logger.error(f"An error occurred during account normalization: {e}", exc_info=True)
        raise

# This block allows the script to be run directly for testing
if __name__ == "__main__":
    run()
