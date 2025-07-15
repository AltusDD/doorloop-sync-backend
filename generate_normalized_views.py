
# generate_normalized_views.py

import os
import sys
import json
import logging
import requests
import traceback

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

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logger.error("❌ CRITICAL: Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY environment variables.")
    raise ValueError("Missing Supabase environment variables.")

def execute_sql_query_requests(sql: str):
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
            logger.error(f"🔽 Response status: {e.response.status_code}")
            logger.error(f"🔽 Response body: {e.response.text}")
        traceback.print_exc()
        raise

def execute_sql_query_supabase(sql: str):
    try:
        logger.info(f"📤 Executing SQL via Supabase client: {sql.splitlines()[0]}...")
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        response = supabase.rpc('execute_sql', {'sql_text': sql}).execute()
        logger.info(f"✅ SQL execution succeeded: {sql.splitlines()[0]}...")
        return response.data
    except Exception as e:
        logger.error(f"❌ Error executing SQL via Supabase client: {type(e).__name__} - {str(e)}")
        traceback.print_exc()
        raise

def execute_sql_query(sql: str):
    if SUPABASE_CLIENT_AVAILABLE:
        return execute_sql_query_supabase(sql)
    else:
        return execute_sql_query_requests(sql)

def main():
    try:
        logger.info("🔍 Starting view generation from raw tables...")
        sql = "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_name LIKE 'doorloop_raw_%' ORDER BY table_name;"
        tables = execute_sql_query(sql)
        logger.info(f"📦 Tables found: {[t['table_name'] for t in tables]}")
    except Exception as e:
        logger.critical(f"🔥 Fatal error in main(): {e}")
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
