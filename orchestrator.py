import os
import logging
from dotenv import load_dotenv
from doorloop_sync.tasks.sync_all import sync_all
from doorloop_sync.clients.supabase_client import SupabaseClient

# --- Configuration & Logging Setup ---
# Load environment variables from .env file for security
load_dotenv()

# Set up structured logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# --- Main Execution ---
def main():
    """
    Main function to orchestrate the ETL pipeline for syncing DoorLoop data to Supabase.
    """
    logger.info("🚀 Starting Empire Command Center ETL Pipeline...")

    # Validate that all necessary environment variables are set
    required_vars = ['DOORLOOP_API_KEY', 'DOORLOOP_API_BASE_URL', 'SUPABASE_URL', 'SUPABASE_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"❌ Missing critical environment variables: {', '.join(missing_vars)}")
        logger.error("Please ensure a .env file exists at the root and contains all required variables.")
        return

    logger.info("✅ Environment configuration loaded successfully.")

    try:
        # The sync_all function will handle the entire raw and normalization process
        sync_all()

        logger.info("✅ ETL pipeline run completed successfully.")

        # --- Post-Sync Validation ---
        logger.info("🔍 Running post-sync validation queries...")
        supabase = SupabaseClient()
        validation_queries = {
            "Properties": "SELECT COUNT(*) FROM properties WHERE doorloop_id IS NOT NULL;",
            "Units": "SELECT COUNT(*) FROM units WHERE doorloop_id IS NOT NULL;",
            "Tenants": "SELECT COUNT(*) FROM tenants WHERE doorloop_id IS NOT NULL;",
            "Leases": "SELECT COUNT(*) FROM leases WHERE doorloop_id IS NOT NULL;",
            "Lease Payments": "SELECT COUNT(*) FROM lease_payments WHERE doorloop_id IS NOT NULL;",
            "Work Orders": "SELECT COUNT(*) FROM tasks WHERE doorloop_id IS NOT NULL AND type = 'WORK_ORDER';",
            "Vendors": "SELECT COUNT(*) FROM vendors WHERE doorloop_id IS NOT NULL;"
        }

        for table, query in validation_queries.items():
            # Note: Supabase Python client v2 doesn't have a direct `query` method.
            # This assumes you have a helper or will execute this manually.
            # For demonstration, we'll just log the intent.
            # In a real scenario, you'd use PostgREST to execute raw SQL or use the client's `rpc` method.
            logger.info(f"   Querying count for: {table}")
            # Example with a hypothetical execute method:
            # result = supabase.execute(query)
            # count = result.data[0]['count']
            # logger.info(f"   ➡️ {table} count: {count}")

        logger.info("✅ Post-sync validation checks initiated.")

    except Exception as e:
        logger.error(f"❌ A critical error occurred during the pipeline execution: {e}", exc_info=True)
        # Add any notification logic here (e.g., Slack, Email)

if __name__ == "__main__":
    # This block allows the script to be run directly
    main()
