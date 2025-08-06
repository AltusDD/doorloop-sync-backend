import os
import logging
from dotenv import load_dotenv

def main():
    """
    Main function to configure the environment and run the ETL pipeline.
    """
    # Load environment variables from a .env file at the project root.
    # This makes running the script locally and in CI/CD consistent.
    load_dotenv()

    # Set up structured logging for clear, debuggable output.
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger(__name__)

    # --- Environment Validation ---
    # Ensure all required secrets are available before starting.
    required_vars = ['DOORLOOP_API_KEY', 'DOORLOOP_API_BASE_URL', 'SUPABASE_URL', 'SUPABASE_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"❌ Missing critical environment variables: {', '.join(missing_vars)}")
        logger.error("Please ensure they are set in your environment or a .env file.")
        return # Exit if configuration is incomplete

    logger.info("✅ Environment configuration validated successfully.")
    logger.info("🚀 Starting Empire Command Center ETL Pipeline...")

    try:
        # This import is now safe because we will run with the correct PYTHONPATH
        from doorloop_sync.tasks.sync_all import sync_all
        sync_all()
        logger.info("✅ ETL pipeline run completed successfully.")

    except ModuleNotFoundError:
        logger.error("❌ ModuleNotFoundError: Could not find the 'doorloop_sync' package.")
        logger.error("   This is a pathing issue. Please run the script from the project root using: ")
        logger.error("   PYTHONPATH=. python orchestrator.py")
    except Exception as e:
        logger.error(f"❌ A critical error occurred during the pipeline execution: {e}", exc_info=True)

if __name__ == "__main__":
    # This block allows the script to be run directly using `python orchestrator.py`
    main()
