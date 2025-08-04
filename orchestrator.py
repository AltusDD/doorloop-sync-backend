import os
import sys
import logging

# Ensure PYTHONPATH includes current directory
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from doorloop_sync.tasks.sync_all import sync_all
from doorloop_sync.clients.supabase_client import SupabaseClient

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def main():
    logger.info("🚀 Starting Empire Command Center ETL Pipeline...")

    required_vars = ['DOORLOOP_API_KEY', 'DOORLOOP_API_BASE_URL', 'SUPABASE_URL', 'SUPABASE_SERVICE_ROLE_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    if missing_vars:
        logger.error(f"❌ Missing critical environment variables: {', '.join(missing_vars)}")
        return

    logger.info("✅ Environment configuration loaded successfully.")

    try:
        sync_all()
        logger.info("✅ ETL pipeline run completed successfully.")
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
            logger.info(f"   Querying count for: {table}")
    except Exception as e:
        logger.error(f"❌ A critical error occurred during the pipeline execution: {e}", exc_info=True)

if __name__ == "__main__":
    main()
