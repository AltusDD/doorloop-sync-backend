import logging
from doorloop_sync.config import get_supabase_client, get_logger

logger = get_logger(__name__)

def run():
    logger.info("Starting normalization for owners...")
    try:
        supabase_client = get_supabase_client()
        raw_table_name = "doorloop_raw_owners"
        normalized_table_name = "doorloop_normalized_owners"
        response = supabase_client.supabase.table(raw_table_name).select("data").execute()
        raw_records = response.data

        if not raw_records:
            logger.info("No raw owners data to normalize. Task complete.")
            return

        unique_raw_records = {
            item['data']['id']: item['data'] for item in raw_records if item.get('data') and item['data'].get('id')
        }.values()

        normalized_records = []
        for record in unique_raw_records:
            normalized_records.append({
                "doorloop_id": record.get("id"),
                "name": record.get("name"),
                "type": record.get("type"),
                "balance": record.get("balance"),
            })

        if normalized_records:
            supabase_client.upsert(table=normalized_table_name, data=normalized_records)
            logger.info(f"Successfully normalized and upserted {len(normalized_records)} owners records.")
    except Exception as e:
        logger.error(f"An error occurred during owners normalization: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run()
