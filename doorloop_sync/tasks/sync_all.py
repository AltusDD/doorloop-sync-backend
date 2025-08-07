import logging
from doorloop_sync.tasks.raw_sync import (
    sync_properties, sync_units, sync_leases, sync_tenants, sync_owners
    # ... add other raw sync imports as needed
)
from doorloop_sync.tasks.normalization.normalize_properties import normalize_properties
from doorloop_sync.tasks.normalization.normalize_units import normalize_units
from doorloop_sync.tasks.normalization.normalize_leases import normalize_leases
from doorloop_sync.services.kpi_service import KpiService

logger = logging.getLogger(__name__)

def sync_all():
    """
    Orchestrates the full ETL pipeline in the correct order:
    1. Raw Data Ingestion
    2. Data Normalization
    3. KPI Calculation
    """
    logger.info("🔁 Starting full DoorLoop sync for all entities...")

    # ======================================================================
    # PHASE 1: Raw Data Ingestion
    # ======================================================================
    raw_sync_tasks = [
        ("Properties", sync_properties), ("Units", sync_units), 
        ("Tenants", sync_tenants), ("Owners", sync_owners), ("Leases", sync_leases)
        # ... add other raw sync tasks as needed
    ]
    logger.info("--- Starting Phase 1: Raw Data Ingestion ---")
    for name, fn in raw_sync_tasks:
        try:
            logger.info(f"🔄 Syncing Raw {name}...")
            fn()
            logger.info(f"✅ Raw {name} sync completed.")
        except Exception as e:
            logger.error(f"❌ Raw {name} sync failed: {e}", exc_info=True)
            raise

    # ======================================================================
    # PHASE 2: Data Normalization
    # ======================================================================
    normalization_tasks = [
        ("Properties", normalize_properties),
        ("Units", normalize_units),
        ("Leases", normalize_leases)
    ]
    logger.info("--- Starting Phase 2: Data Normalization ---")
    for name, fn in normalization_tasks:
        try:
            logger.info(f"🔄 Normalizing {name}...")
            fn()
            logger.info(f"✅ {name} normalization completed.")
        except Exception as e:
            logger.error(f"❌ {name} normalization failed: {e}", exc_info=True)
            raise

    # ======================================================================
    # PHASE 3: KPI Calculation
    # ======================================================================
    logger.info("--- Starting Phase 3: KPI Calculation ---")
    try:
        KpiService.compute_and_store_all_kpis()
        logger.info("✅ KPI calculation completed.")
    except Exception as e:
        logger.error(f"❌ KPI calculation failed: {e}", exc_info=True)
        raise

    logger.info("🏁 All ETL tasks completed successfully.")