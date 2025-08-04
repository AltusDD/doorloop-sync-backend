
import logging
from doorloop_sync.clients.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

def normalize_units():
    logger.info("🔄 Normalizing units...")
    # Placeholder logic
    supabase = SupabaseClient()
    logger.info("✅ Units normalization complete.")
