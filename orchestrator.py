import logging
from doorloop_sync.tasks.sync_all import sync_all

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    logger.info("🚀 Starting Empire Command Center ETL Pipeline...")
    try:
        sync_all()
    except Exception as e:
        logger.error(f"❌ A critical error occurred during the pipeline execution: {e}", exc_info=True)

if __name__ == "__main__":
    main()
