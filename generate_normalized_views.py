# generate_normalized_views.py

import os
import sys
import json
import logging
import requests

# Try to import Supabase client, but provide a fallback
try:
    from supabase import create_client, Client
    SUPABASE_CLIENT_AVAILABLE = True
except ImportError:
    SUPABASE_CLIENT_AVAILABLE = False
    logging.warning("Supabase client not installed. Falling back to requests-based method.")

# Setup logging
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Supabase Configuration
SUPABASE_URL = os.environ.get("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

# Validate environment variables
if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logger.error("❌ CRITICAL: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")
    raise ValueError("Missing Supabase environment variables.")

# Fallback client if Supabase client is not available
def execute_sql_query_requests(sql: str):
    """
    Execute SQL query using requests when Supabase client is not available
    """
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    
    payload = {"sql_text": sql}
    
    try:
        logger.info(f"📤 Executing SQL via requests: {sql.splitlines()[0]}...")
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        
        data = response.json()
        logger.info(f"✅ SQL execution succeeded: {sql.splitlines()[0]}...")
        return data
    
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ Error executing SQL: {type(e).__name__} - {str(e)}")
        if hasattr(e, 'response'):
            logger.error(f"Response text: {e.response.text}")
        raise

# Supabase client method
def execute_sql_query_supabase(sql: str):
    """
    Execute SQL query using Supabase client
    """
    try:
        logger.info(f"📤 Executing SQL via Supabase client: {sql.splitlines()[0]}...")
        
        # Use the rpc method with the correct function name and parameter
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        response = supabase.rpc('execute_sql', {'sql_text': sql}).execute()
        
        logger.info(f"✅ SQL execution succeeded: {sql.splitlines()[0]}...")
        return response.data
    
    except Exception as e:
        logger.error(f"❌ Error executing SQL: {type(e).__name__} - {str(e)}")
        raise

# Choose the appropriate execution method
def execute_sql_query(sql: str):
    if SUPABASE_CLIENT_AVAILABLE:
        return execute_sql_query_supabase(sql)
    else:
        return execute_sql_query_requests(sql)

# Rest of the script remains the same as in the previous response...
# (Include the entire previous script from get_raw_table_names() onwards)

def run():
    """
    Main execution function to generate normalized views.
    """
    try:
        # Get raw table names
        raw_tables = get_raw_table_names()
        
        if not raw_tables:
            logger.warning("⚠️ No raw tables found. Exiting.")
            return

        # Process each raw table
        for table in raw_tables:
            logger.info(f"🔄 Processing {table}...")
            try:
                # Get columns for the table
                columns = get_table_columns(table)

                if not columns:
                    logger.warning(f"⚠️ No columns found for {table}. Skipping view creation.")
                    continue

                # Create view
                create_or_replace_view(table, columns)

            except Exception as e:
                logger.error(f"❌ Failed to process {table} for view generation: {type(e).__name__}: {e}")
                continue 

    except Exception as e:
        logger.error(f"❌ Critical error during view generation: {e}")
        raise

if __name__ == "__main__":
    # Check and install dependencies if not available
    try:
        import supabase
    except ImportError:
        logger.warning("Supabase client not found. Attempting to install...")
        import subprocess
        subprocess.check_call([sys.executable, '-m', 'pip', 'install', 'supabase'])
    
    run()