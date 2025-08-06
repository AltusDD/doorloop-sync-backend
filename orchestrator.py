import os
import sys
import logging
from dotenv import load_dotenv

def configure_path():
    """
    Adds the project root directory to the Python path to ensure
    that the 'doorloop_sync' package can be found.
    """
    project_root = os.path.dirname(os.path.abspath(__file__))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

def main():
    """
    Main function to configure the environment and run the ETL pipeline.
    """
    configure_path()
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger(__name__)

    logger.info("🚀 Starting Empire Command Center ETL Pipeline...")
    try:
        from doorloop_sync.tasks.sync_all import sync_all
        sync_all()
        logger.info("✅ ETL pipeline run completed successfully.")
    except Exception as e:
        logger.error(f"❌ A critical error occurred: {e}", exc_info=True)

if __name__ == "__main__":
    main()

# [silent-fix] orchestrator.py final Gemini path patch
