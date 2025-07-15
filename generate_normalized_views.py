# generate_normalized_views.py

import os
import json
import logging
import requests
from supabase import create_client, Client

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

# Initialize Supabase client
supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

def execute_sql_query(sql: str):
    """
    Executes SQL queries via Supabase RPC with robust error handling.
    """
    try:
        logger.info(f"📤 Executing SQL: {sql.splitlines()[0]}...")
        
        # Use the rpc method with the correct function name and parameter
        response = supabase.rpc('execute_sql', {'sql_text': sql}).execute()
        
        logger.info(f"✅ SQL execution succeeded: {sql.splitlines()[0]}...")
        return response.data
    
    except Exception as e:
        logger.error(f"❌ Error executing SQL: {type(e).__name__} - {str(e)}")
        logger.error(f"SQL Query: {sql}")
        raise

def get_raw_table_names():
    """
    Retrieve raw table names from the database.
    Fallback to hardcoded list if query fails.
    """
    sql = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'public' AND table_name LIKE 'doorloop_raw_%'
    ORDER BY table_name;
    """
    try:
        table_rows = execute_sql_query(sql)
        return [row['table_name'] for row in table_rows]
    except Exception as e:
        logger.warning(f"⚠️ Falling back to hardcoded table list due to error: {e}")
        return [
            "doorloop_raw_properties", "doorloop_raw_units", "doorloop_raw_leases", 
            "doorloop_raw_tenants", "doorloop_raw_lease_payments", "doorloop_raw_lease_charges", 
            "doorloop_raw_owners", "doorloop_raw_vendors", "doorloop_raw_tasks", 
            "doorloop_raw_files", "doorloop_raw_notes", "doorloop_raw_communications",
            "doorloop_raw_applications", "doorloop_raw_inspections", 
            "doorloop_raw_insurance_policies", "doorloop_raw_recurring_charges", 
            "doorloop_raw_recurring_credits", "doorloop_raw_accounts", "doorloop_raw_users", 
            "doorloop_raw_portfolios", "doorloop_raw_reports", "doorloop_raw_activity_logs",
        ]

def get_table_columns(table_name: str):
    """
    Retrieve columns for a given table, excluding specific columns.
    """
    sql = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'public' AND table_name = '{table_name}'
    ORDER BY ordinal_position;
    """
    
    # Extensive list of columns to exclude
    columns_to_exclude = {
        "data", "leaseDepositItem", "leasechargeitem", "totalbalance", "register", "tags", 
        "taxable", "openedat", "linkedresource", "defaultaccountfor", "sentat", "bouncedat", 
        "lines", "bcc", "acceptedontos", "duedate", "portalinfo", "rank", 
        "conversationwelcomesmsentat", "from", "prospectinfo", "pets", "metadata", 
        # ... (rest of your existing excluded columns)
    }

    try:
        data = execute_sql_query(sql)
        
        # Handle different response formats
        if not data:
            logger.warning(f"No columns found for {table_name}")
            return []

        # Extract column names, filtering out excluded columns
        filtered_columns = [
            row['column_name'] for row in data 
            if row['column_name'].lower() not in columns_to_exclude
        ]

        # Essential columns to always include
        essential_columns = {"id", "doorloop_id", "payload_json", "created_at", "updated_at", "_raw_payload"}
        
        # Combine and sort columns
        final_columns = list(set(filtered_columns) | essential_columns)
        final_columns.sort()

        logger.info(f"✅ Columns used in view for {table_name}: {final_columns}")
        return final_columns

    except Exception as e:
        logger.error(f"❌ Failed to fetch columns for {table_name}: {e}")
        raise

def create_or_replace_view(raw_table_name: str, columns: list):
    """
    Create or replace a view for a given raw table.
    """
    view_name = raw_table_name.replace("doorloop_raw_", "")
    quoted_columns = [f'"{col}"' for col in columns]
    select_clause = ", ".join(quoted_columns)

    view_sql = f"""
CREATE OR REPLACE VIEW public."{view_name}" AS
SELECT
    {select_clause}
FROM public."{raw_table_name}";
"""
    
    try:
        logger.info(f"🏗️ Creating view: {view_name}")
        execute_sql_query(view_sql)
        logger.info(f"✅ Successfully created view: {view_name}")
    except Exception as e:
        logger.error(f"❌ Failed to create view {view_name}: {e}")
        raise

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
    run()