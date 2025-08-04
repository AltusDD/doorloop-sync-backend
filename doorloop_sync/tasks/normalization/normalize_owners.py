
import logging
from doorloop_sync.clients.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

def normalize_owners():
    logger.info("🔄 Normalizing owners...")
    # Placeholder logic
    supabase = SupabaseClient()
    logger.info("✅ Owners normalization complete.")
