
import logging
from doorloop_sync.tasks.normalization.normalize_properties import normalize_properties
from doorloop_sync.tasks.normalization.normalize_units import normalize_units
from doorloop_sync.tasks.normalization.normalize_owners import normalize_owners
from doorloop_sync.tasks.normalization.normalize_tenants import normalize_tenants
from doorloop_sync.tasks.normalization.normalize_leases import normalize_leases
from doorloop_sync.tasks.normalization.normalize_lease_payments import normalize_lease_payments

logger = logging.getLogger(__name__)

def normalize_all():
    logger.info("📦 Starting normalization process...")
    normalize_properties()
    normalize_units()
    normalize_owners()
    normalize_tenants()
    normalize_leases()
    normalize_lease_payments()
    logger.info("✅ Normalization complete.")
