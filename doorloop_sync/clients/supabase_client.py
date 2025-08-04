from supabase import create_client, Client
from postgrest.exceptions import APIError
import logging
import os
import json

logger = logging.getLogger(__name__)

def standardize_records(records: list) -> list[dict]:
    """
    Ensures all dictionaries in a list have the same set of keys.
    It finds all unique keys across all valid records and adds any missing keys
    to each record with a value of None. It safely ignores any items in the
    list that are not dictionaries.
    """
    if not records:
        return []

    # FIX: Filter out any items that are not dictionaries to prevent AttributeErrors.
    valid_records = [r for r in records if isinstance(r, dict)]

    if not valid_records:
        logger.warning("No valid dictionary records found in payload to standardize.")
        return []

    # Use a set to collect all unique keys from all valid records
    all_keys = set()
    for record in valid_records:
        all_keys.update(record.keys())

    # Create a new list of standardized records
    standardized = []
    for record in valid_records:
        new_record = {}
        for key in all_keys:
            new_record[key] = record.get(key)
        standardized.append(new_record)
        
    return standardized

class SupabaseClient:
    def __init__(self):
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")
        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY must be set.")
        self.supabase: Client = create_client(url, key)

    def upsert(self, table: str, data: list):
        if not data:
            logger.warning(f"⏭️ Skipping upsert to '{table}' — payload was empty.")
            return {}

        standardized_data = standardize_records(data)
        
        if not standardized_data:
            logger.warning(f"⏭️ Skipping upsert to '{table}' — no valid records after standardization.")
            return {}

        try:
            response = self.supabase.table(table).upsert(standardized_data).execute()
            logger.info(f"✅ Successfully upserted {len(standardized_data)} records to '{table}'.")
            return response
        except APIError as e:
            logger.error(f"❌ Supabase APIError on upsert to '{table}' with {len(standardized_data)} records.")
            logger.error(f"   Error Message: {e.message}")
            if standardized_data:
                logger.error(f"   Sample Payload Keys: {list(standardized_data[0].keys())}")
            raise
        except Exception as e:
            logger.error(f"❌ An unexpected error occurred during upsert to '{table}'.")
            logger.exception(e)
            raise

    def fetch_all(self, table: str):
        try:
            response = self.supabase.table(table).select("*").execute()
            return response.data
        except Exception as e:
            logger.error(f"Failed to fetch data from table '{table}': {e}")
            return []
