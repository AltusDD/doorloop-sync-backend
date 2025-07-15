import os
import sys
import logging
import requests

# Setup logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load from environment variables
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    logger.critical("❌ SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY not set in environment.")
    sys.exit(1)

# Try importing Supabase client
try:
    from supabase import create_client, Client
    SUPABASE_CLIENT_AVAILABLE = True
except ImportError:
    SUPABASE_CLIENT_AVAILABLE = False
    logger.warning("Supabase client not installed. Falling back to requests-based method.")

def execute_sql_query_requests(sql: str):
    """Execute SQL using raw HTTP POST."""
    url = f"{SUPABASE_URL}/rest/v1/rpc/execute_sql"
    headers = {
        "apikey": SUPABASE_SERVICE_ROLE_KEY,
        "Authorization": f"Bearer {SUPABASE_SERVICE_ROLE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=representation"
    }
    payload = {"sql_text": sql}

    try:
        logger.info(f"📤 Executing SQL via requests: {sql.strip().splitlines()[0]}...")
        response = requests.post(url, headers=headers, json=payload, timeout=60)
        response.raise_for_status()
        data = response.json()
        logger.info("✅ SQL executed successfully (requests)")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"❌ HTTP error ({type(e).__name__}): {e}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response text: {e.response.text}")
        raise

def execute_sql_query_supabase(sql: str):
    """Execute SQL using Supabase client."""
    try:
        logger.info(f"📤 Executing SQL via Supabase client: {sql.strip().splitlines()[0]}...")
        client: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)
        response = client.rpc("execute_sql", {"sql_text": sql}).execute()
        logger.info("✅ SQL executed successfully (supabase client)")
        return response.data
    except Exception as e:
        logger.error(f"❌ Supabase client error ({type(e).__name__}): {e}")
        raise

def execute_sql_query(sql: str):
    if SUPABASE_CLIENT_AVAILABLE:
        return execute_sql_query_supabase(sql)
    else:
        return execute_sql_query_requests(sql)

def run():
    logger.info("🔍 Starting view generation from raw tables...")

    try:
        result = execute_sql_query("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_name LIKE 'doorloop_raw_%'
        ORDER BY table_name;
        """)
        raw_tables = [row["table_name"] for row in result]
    except Exception as e:
        logger.warning(f"⚠️ Failed to fetch raw table list dynamically. Using fallback list. Reason: {e}")
        raw_tables = [
            "doorloop_raw_properties",
            "doorloop_raw_units",
            "doorloop_raw_leases",
            "doorloop_raw_tenants",
            "doorloop_raw_lease_payments",
            "doorloop_raw_lease_charges",
            "doorloop_raw_owners",
            "doorloop_raw_vendors",
        ]

    for table in raw_tables:
        try:
            logger.info(f"🔄 Processing {table}...")
            result = execute_sql_query(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_name = '{table}'
            ORDER BY ordinal_position;
            """)
            column_names = [row["column_name"] for row in result]
            logger.info(f"🧱 {table}: {len(column_names)} columns found")
        except Exception as e:
            logger.error(f"❌ Failed to fetch columns for {table}: {e}")

if __name__ == "__main__":
    try:
        run()
    except Exception as e:
        logger.critical(f"❌ Unhandled fatal error: {type(e).__name__} - {e}")
        if hasattr(e, "response"):
            logger.error(f"Response text: {e.response.text}")
        sys.exit(1)
