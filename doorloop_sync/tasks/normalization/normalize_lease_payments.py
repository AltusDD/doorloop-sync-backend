
import logging
from doorloop_sync.clients.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

def normalize_lease_payments():
    logger.info("🔄 Normalizing lease payments...")
    # Placeholder logic
    supabase = SupabaseClient()
    logger.info("✅ Lease payments normalization complete.")
