
import logging
from doorloop_sync.clients.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

def normalize_properties():
    logger.info("🔄 Normalizing properties...")
    # Placeholder logic
    supabase = SupabaseClient()
    logger.info("✅ Properties normalization complete.")
