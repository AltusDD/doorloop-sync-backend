import os
import sys
import logging
from dotenv import load_dotenv

def configure_path():
    """
    Adds the project root directory to the Python path to ensure
    that the 'doorloop_sync' package can be found.
    """
    # Get the absolute path of the directory containing this script (the project root)
    project_root = os.path.dirname(os.path.abspath(__file__))
    
    # Add the project root to the system path if it's not already there
    if project_root not in sys.path:
        sys.path.insert(0, project_root)
        print(f"✅ Project root added to Python path: {project_root}")

def main():
    """
    Main function to configure the environment and run the ETL pipeline.
    """
    # This function MUST be called first to fix the import path.
    configure_path()

    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger(__name__)

    required_vars = ['DOORLOOP_API_KEY', 'DOORLOOP_API_BASE_URL', 'SUPABASE_URL', 'SUPABASE_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"❌ Missing critical environment variables: {', '.join(missing_vars)}")
        return

    logger.info("✅ Environment configuration validated successfully.")
    logger.info("🚀 Starting Empire Command Center ETL Pipeline...")

    try:
        # This import will now succeed because the path has been configured.
        from doorloop_sync.tasks.sync_all import sync_all
        sync_all()
        logger.info("✅ ETL pipeline run completed successfully.")

    except Exception as e:
        logger.error(f"❌ A critical error occurred during the pipeline execution: {e}", exc_info=True)

if __name__ == "__main__":
    main()
