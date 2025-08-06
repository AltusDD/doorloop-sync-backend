import logging
# Correct, absolute imports from the 'raw_sync' sub-package
from doorloop_sync.tasks.raw_sync.sync_properties import sync_properties
from doorloop_sync.tasks.raw_sync.sync_units import sync_units
from doorloop_sync.tasks.raw_sync.sync_tenants import sync_tenants
from doorloop_sync.tasks.raw_sync.sync_leases import sync_leases
from doorloop_sync.tasks.raw_sync.sync_tasks import sync_tasks
# ... import all other necessary sync functions here

logger = logging.getLogger(__name__)

def sync_all():
    """
    Orchestrates the raw data sync for all DoorLoop entities.
    """
    sync_jobs = {
        "properties": sync_properties,
        "units": sync_units,
        "tenants": sync_tenants,
        "leases": sync_leases,
        "tasks": sync_tasks,
        # ... add all other job functions here
    }

    for entity, job_function in sync_jobs.items():
        logger.info(f"🚀 Starting sync for: {entity}")
        try:
            job_function()
        except Exception as e:
            logger.error(f"❌ ERROR during sync for {entity}", exc_info=True)

