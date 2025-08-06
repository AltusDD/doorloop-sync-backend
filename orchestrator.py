import os
import logging
from doorloop_sync.tasks.sync_all import sync_all

# Setup basic logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def validate_environment():
    required_env_vars = [
        "SUPABASE_SERVICE_ROLE_KEY",
        "SUPABASE_URL",
        "DOORLOOP_API_KEY",
        "DOORLOOP_API_BASE_URL"
    ]
    missing = [var for var in required_env_vars if not os.getenv(var)]
    if missing:
        raise EnvironmentError(f"❌ Missing critical environment variables: {', '.join(missing)}")
    logger.info("✅ Environment configuration validated successfully.")

def main():
    try:
        validate_environment()
        logger.info("🚀 Starting Empire Command Center ETL Pipeline...")
        sync_all()
        logger.info("✅ ETL pipeline run completed successfully.")
    except Exception as e:
        logger.error(f"❌ A critical error occurred during the pipeline execution: {e}", exc_info=True)

if __name__ == "__main__":
    main()
