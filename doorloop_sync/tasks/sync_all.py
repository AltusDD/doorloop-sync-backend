import logging

from doorloop_sync.tasks.raw_sync.sync_properties import sync_properties
from doorloop_sync.tasks.raw_sync.sync_units import sync_units
from doorloop_sync.tasks.raw_sync.sync_leases import sync_leases
from doorloop_sync.tasks.raw_sync.sync_tenants import sync_tenants
from doorloop_sync.tasks.raw_sync.sync_owners import sync_owners
from doorloop_sync.tasks.raw_sync.sync_payments import sync_payments
from doorloop_sync.tasks.raw_sync.sync_tasks import sync_tasks  # Tasks = work orders
from doorloop_sync.tasks.raw_sync.sync_vendors import sync_vendors

from doorloop_sync.services.kpi_service import KpiService

logger = logging.getLogger(__name__)

def sync_all():
    logger.info("🔁 Starting full DoorLoop sync...")

    sync_tasks_list = [
        ("Properties", sync_properties),
        ("Units", sync_units),
        ("Leases", sync_leases),
        ("Tenants", sync_tenants),
        ("Owners", sync_owners),
        ("Payments", sync_payments),
        ("Tasks", sync_tasks),
        ("Vendors", sync_vendors),
    ]

    for name, fn in sync_tasks_list:
        try:
            logger.info(f"🔄 Syncing {name}...")
            fn()
            logger.info(f"✅ {name} sync completed.")
        except Exception as e:
            logger.error(f"❌ {name} sync failed: {e}", exc_info=True)

    # Run KPIs at the end
    try:
        logger.info("📊 Running KPI calculation...")
        KpiService.compute_and_store_all_kpis()
        logger.info("✅ KPI calculation completed.")
    except Exception as e:
        logger.error(f"❌ KPI calculation failed: {e}", exc_info=True)

    logger.info("🏁 All sync tasks complete.")

