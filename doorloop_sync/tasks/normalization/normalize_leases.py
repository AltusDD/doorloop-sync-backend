
import logging
from doorloop_sync.clients.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

def normalize_leases():
    logger.info("🔄 Normalizing leases...")
    # Placeholder logic
    supabase = SupabaseClient()
    logger.info("✅ Leases normalization complete.")
