import logging
# --- Import all raw sync functions ---
from doorloop_sync.tasks.raw_sync import (
    sync_accounts, sync_activity_logs, sync_applications, sync_communications,
    sync_files, sync_inspections, sync_insurance_policies, sync_leases,
    sync_lease_charges, sync_lease_credits, sync_lease_payments, sync_notes,
    sync_owners, sync_portfolios, sync_properties, sync_tasks,
    sync_tenants, sync_units, sync_users, sync_vendors
)
# --- Import the correct KPI service ---
from doorloop_sync.services.kpi_service import KpiService

logger = logging.getLogger(__name__)

def sync_all():
    """
    Orchestrates the full ETL pipeline: raw syncs in dependency order,
    followed by KPI calculations.
    """
    logger.info("🔁 Starting full DoorLoop sync for all entities...")

    sync_tasks_list = [
        ("Accounts", sync_accounts), ("Users", sync_users), ("Portfolios", sync_portfolios),
        ("Vendors", sync_vendors), ("Owners", sync_owners), ("Properties", sync_properties),
        ("Units", sync_units), ("Tenants", sync_tenants), ("Leases", sync_leases),
        ("Lease Charges", sync_lease_charges), ("Lease Credits", sync_lease_credits),
        ("Lease Payments", sync_lease_payments), ("Tasks", sync_tasks),
        ("Communications", sync_communications), ("Notes", sync_notes), ("Files", sync_files),
        ("Insurance Policies", sync_insurance_policies), ("Inspections", sync_inspections),
        ("Applications", sync_applications), ("Activity Logs", sync_activity_logs)
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
