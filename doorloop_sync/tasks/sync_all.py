# Imports from the specific 'raw_sync' sub-directory
from doorloop_sync.tasks.raw_sync.sync_properties import sync_properties
from doorloop_sync.tasks.raw_sync.sync_units import sync_units
from doorloop_sync.tasks.raw_sync.sync_owners import sync_owners
from doorloop_sync.tasks.raw_sync.sync_tenants import sync_tenants
from doorloop_sync.tasks.raw_sync.sync_leases import sync_leases
from doorloop_sync.tasks.raw_sync.sync_lease_payments import sync_lease_payments
from doorloop_sync.tasks.raw_sync.sync_vendors import sync_vendors
from doorloop_sync.tasks.raw_sync.sync_files import sync_files
from doorloop_sync.tasks.raw_sync.sync_notes import sync_notes
# CORRECT: Only import sync_tasks, as it handles all task types including work orders
from doorloop_sync.tasks.raw_sync.sync_tasks import sync_tasks

def sync_all():
    """
    Orchestrates the raw data sync for all DoorLoop entities in the correct sequence.
    """
    # A dictionary to hold all sync functions for easy iteration
    sync_jobs = {
        "properties": sync_properties,
        "units": sync_units,
        "owners": sync_owners,
        "tenants": sync_tenants,
        "leases": sync_leases,
        "lease_payments": sync_lease_payments,
        "vendors": sync_vendors,
        "files": sync_files,
        "notes": sync_notes,
        # CORRECT: Only one call to sync all tasks
        "tasks": sync_tasks,
    }

    for entity, job_function in sync_jobs.items():
        print(f"🚀 Starting sync for: {entity}")
        try:
            job_function()
        except Exception as e:
            # Using a logger is better, but print is fine for now
            print(f"❌ ERROR during sync for {entity}: {e}")

