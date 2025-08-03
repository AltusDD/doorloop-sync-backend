import logging
from doorloop_sync.config import get_supabase_client, get_logger

logger = get_logger(__name__)

def run():
    """
    Normalizes raw file data and upserts it into the normalized table.
    """
    logger.info("Starting normalization for files...")

    try:
        supabase_client = get_supabase_client()
        raw_table_name = "doorloop_raw_files"
        normalized_table_name = "doorloop_normalized_files"

        # 1. Fetch raw data
        response = supabase_client.supabase.table(raw_table_name).select("data").execute()
        raw_records = response.data

        if not raw_records:
            logger.info("No raw file data to normalize.")
            return

        # 2. Deduplicate and normalize
        unique_raw_records = {
            item['data']['id']: item['data'] for item in raw_records if item.get('data') and item['data'].get('id')
        }.values()

        normalized_records = []
        for record in unique_raw_records:
            normalized_records.append({
                "doorloop_id": record.get("id"),
                "name": record.get("name"),
                "entity_id": record.get("entityId"),
                "entity_type": record.get("entityType"),
                "mime_type": record.get("mimeType"),
                "file_size": record.get("fileSize"),
                "created_at": record.get("createdAt"),
                "updated_at": record.get("updatedAt"),
            })

        # 3. Upsert normalized data
        if normalized_records:
            supabase_client.upsert(table=normalized_table_name, data=normalized_records)
            logger.info(f"Successfully normalized and upserted {len(normalized_records)} file records.")

    except Exception as e:
        logger.error(f"Error during file normalization: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    run()
