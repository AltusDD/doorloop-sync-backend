from supabase import create_client, Client
from postgrest.exceptions import APIError
import logging
import os

class SupabaseClient:
    def __init__(self):
        """
        Initializes the Supabase client using credentials from environment variables.
        """
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY environment variables must be set.")

        self.supabase: Client = create_client(url, key)

    def upsert(self, table: str, data: list[dict]) -> dict:
        """
        Upserts a list of records into a specified Supabase table.

        Args:
            table: The name of the table to upsert into.
            data: A list of dictionaries representing the records.

        Returns:
            The API response from Supabase or an empty dict if the operation was skipped or failed.
        """
        # FIX: Add a guard clause to prevent errors from empty payloads.
        if not data:
            logging.warning(f"⏭️ Skipping upsert to '{table}' — payload was empty.")
            return {}

        # FIX: Wrap the API call in a try/except block for robust error handling.
        try:
            response = self.supabase.table(table).upsert(data).execute()
            logging.info(f"✅ Successfully upserted {len(data)} records to '{table}'.")
            return response
        except APIError as e:
            # Log detailed, actionable error information.
            logging.error(f"❌ Supabase APIError on upsert to '{table}' with {len(data)} records.")
            logging.error(f"   Error Message: {e.message}")
            # Log the first record to help identify schema mismatches or bad data.
            if data:
                logging.error(f"   Sample Payload Record: {data[0]}")
            # Re-raise the exception to allow upstream handlers to catch it if needed.
            raise
        except Exception as e:
            logging.error(f"❌ An unexpected error occurred during upsert to '{table}'.")
            logging.exception(e) # Logs the full traceback
            raise

    def fetch_all(self, table: str):
        """
        Fetches all records from a specified table.
        """
        try:
            response = self.supabase.table(table).select("*").execute()
            return response.data
        except Exception as e:
            logging.error(f"Failed to fetch data from table '{table}': {e}")
            return []
