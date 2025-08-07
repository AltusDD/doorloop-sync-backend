import logging
# --- Import Core Sync Functions ---
from doorloop_sync.tasks.raw_sync.sync_properties import sync_properties
from doorloop_sync.tasks.raw_sync.sync_units import sync_units
from doorloop_sync.tasks.raw_sync.sync_owners import sync_owners
from doorloop_sync.tasks.raw_sync.sync_tenants import sync_tenants
from doorloop_sync.tasks.raw_sync.sync_leases import sync_leases
from doorloop_sync.tasks.raw_sync.sync_vendors import sync_vendors
from doorloop_sync.tasks.raw_sync.sync_tasks import sync_tasks

# --- Import Financial Sync Functions ---
# This is the function you are using
from doorloop_sync.tasks.raw_sync.sync_lease_payments import sync_lease_payments
from doorloop_sync.tasks.raw_sync.sync_lease_charges import sync_lease_charges
from doorloop_sync.tasks.raw_sync.sync_lease_credits import sync_lease_credits
from doorloop_sync.tasks.raw_sync.sync_accounts import sync_accounts

# --- Import Supporting Data Sync Functions ---
from doorloop_sync.tasks.raw_sync.sync_files import sync_files
from doorloop_sync.tasks.raw_sync.sync_notes import sync_notes
from doorloop_sync.tasks.raw_sync.sync_communications import sync_communications
from doorloop_sync.tasks.raw_sync.sync_users import sync_users
from doorloop_sync.tasks.raw_sync.sync_portfolios import sync_portfolios

# --- Import Post-Sync Services ---
from doorloop_sync.services.kpi_service import KpiService

logger = logging.getLogger(__name__)

def sync_all():
    """
    Orchestrates the full ETL pipeline for 100% ingestion, running raw syncs
    in dependency order, followed by KPI calculations.
    """
    logger.info("🔁 Starting full DoorLoop sync for all entities...")

    sync_tasks_list = [
        # Phase 1: Core Independent Entities
        ("Accounts", sync_accounts),
        ("Users", sync_users),
        ("Portfolios", sync_portfolios),
        ("Vendors", sync_vendors),
        ("Owners", sync_owners),
        ("Properties", sync_properties),
        
        # Phase 2: Core Dependent Entities
        ("Units", sync_units),
        ("Tenants", sync_tenants),

        # Phase 3: Linking Entities (Leases are central)
        ("Leases", sync_leases),

        # Phase 4: Lease-Related Financial Transactions
        ("Lease Charges", sync_lease_charges),
        ("Lease Credits", sync_lease_credits),
        # --- THIS IS THE FIX ---
        # The function name must match the import: sync_lease_payments
        ("Lease Payments", sync_lease_payments),
        
        # Phase 5: Operational & Supporting Data
        ("Tasks (incl. Work Orders)", sync_tasks),
        ("Communications", sync_communications),
        ("Notes", sync_notes),
        ("Files", sync_files),
    ]

    # --- Phase 1: Raw Data Ingestion ---
    for name, fn in sync_tasks_list:
        try:
            logger.info(f"🔄 Syncing {name}...")
            fn()
            logger.info(f"✅ {name} sync completed.")
        except Exception as e:
            logger.error(f"❌ {name} sync failed: {e}", exc_info=True)

    # --- Phase 2: Post-Sync Derivations ---
    try:
        logger.info("📊 Running KPI calculation...")
        KpiService.compute_and_store_all_kpis()
        logger.info("✅ KPI calculation completed.")
    except Exception as e:
        logger.error(f"❌ KPI calculation failed: {e}", exc_info=True)

    logger.info("🏁 All sync tasks complete.")

