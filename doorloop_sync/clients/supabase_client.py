from supabase import create_client, Client
from postgrest.exceptions import APIError
import logging
import os
import json

logger = logging.getLogger(__name__)

def standardize_records(records: list[dict]) -> list[dict]:
    """
    Ensures all dictionaries in a list have the same set of keys.
    It finds all unique keys across all records and adds any missing keys
    to each record with a value of None.
    """
    if not records:
        return []

    # Use a set to collect all unique keys from all records
    all_keys = set()
    for record in records:
        all_keys.update(record.keys())

    # Create a new list of standardized records
    standardized = []
    for record in records:
        new_record = {}
        for key in all_keys:
            # Add the key with its value or None if it's missing
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

    def upsert(self, table: str, data: list[dict]):
        if not data:
            logger.warning(f"⏭️ Skipping upsert to '{table}' — payload was empty.")
            return {}

        # FIX: Standardize the records before sending them to Supabase.
        # This resolves the "All object keys must match" error.
        standardized_data = standardize_records(data)

        try:
            response = self.supabase.table(table).upsert(standardized_data).execute()
            logger.info(f"✅ Successfully upserted {len(standardized_data)} records to '{table}'.")
            return response
        except APIError as e:
            logger.error(f"❌ Supabase APIError on upsert to '{table}' with {len(standardized_data)} records.")
            logger.error(f"   Error Message: {e.message}")
            if standardized_data:
                # Log the keys of a sample record to help debug schema issues
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
