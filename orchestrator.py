import os
import sys
import logging

# --- Path Configuration (MUST RUN FIRST) ---
# This block ensures that the 'doorloop_sync' package can be found by Python
# from the moment the script is loaded.
project_root = os.path.dirname(os.path.abspath(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)
    print(f"✅ Project root added to Python path: {project_root}")
# --- End Path Configuration ---

# Now that the path is set, we can safely import our other modules.
from dotenv import load_dotenv
from doorloop_sync.tasks.sync_all import sync_all

def main():
    """
    Main function to configure the environment and run the ETL pipeline.
    """
    load_dotenv()

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    logger = logging.getLogger(__name__)

    # Use SUPABASE_KEY as it's the more common name for the service role key
    required_vars = ['DOORLOOP_API_KEY', 'DOORLOOP_API_BASE_URL', 'SUPABASE_URL', 'SUPABASE_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error(f"❌ Missing critical environment variables: {', '.join(missing_vars)}")
        return

    logger.info("✅ Environment configuration validated successfully.")
    logger.info("🚀 Starting Empire Command Center ETL Pipeline...")

    try:
        sync_all()
        logger.info("✅ ETL pipeline run completed successfully.")
    except Exception as e:
        logger.error(f"❌ A critical error occurred during the pipeline execution: {e}", exc_info=True)

if __name__ == "__main__":
    main()

# orchestrator.py [silent tag]
# Orchestrator logic
