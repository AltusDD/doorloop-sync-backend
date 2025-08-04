import logging
from doorloop_sync.tasks.raw_sync.sync_accounts import sync_accounts
from doorloop_sync.tasks.raw_sync.sync_properties import sync_properties
from doorloop_sync.tasks.raw_sync.sync_units import sync_units
from doorloop_sync.tasks.raw_sync.sync_tenants import sync_tenants
from doorloop_sync.tasks.raw_sync.sync_leases import sync_leases
from doorloop_sync.tasks.raw_sync.sync_lease_payments import sync_lease_payments
from doorloop_sync.tasks.raw_sync.sync_tasks import sync_tasks
from doorloop_sync.tasks.raw_sync.sync_vendors import sync_vendors
from doorloop_sync.tasks.raw_sync.sync_users import sync_users
from doorloop_sync.tasks.raw_sync.sync_notes import sync_notes

logger = logging.getLogger(__name__)

def sync_all():
    logger.info("🔁 Starting full sync process...")
    sync_accounts()
    sync_properties()
    sync_units()
    sync_tenants()
    sync_leases()
    sync_lease_payments()
    sync_tasks()
    sync_vendors()
    sync_users()
    sync_notes()
    logger.info("✅ Sync complete.")
