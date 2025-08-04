
import logging
from doorloop_sync.clients.supabase_client import SupabaseClient

logger = logging.getLogger(__name__)

def normalize_tenants():
    logger.info("🔄 Normalizing tenants...")
    # Placeholder logic
    supabase = SupabaseClient()
    logger.info("✅ Tenants normalization complete.")
