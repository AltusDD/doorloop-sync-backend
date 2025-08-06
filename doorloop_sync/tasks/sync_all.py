from doorloop_sync.tasks.sync_properties import sync_properties
from doorloop_sync.tasks.sync_units import sync_units
from doorloop_sync.tasks.sync_owners import sync_owners
from doorloop_sync.tasks.sync_tenants import sync_tenants
from doorloop_sync.tasks.sync_leases import sync_leases
from doorloop_sync.tasks.sync_lease_payments import sync_lease_payments
from doorloop_sync.tasks.sync_work_orders import sync_work_orders
from doorloop_sync.tasks.sync_vendors import sync_vendors
from doorloop_sync.tasks.sync_files import sync_files
from doorloop_sync.tasks.sync_notes import sync_notes
from doorloop_sync.tasks.sync_tasks import sync_tasks

def sync_all():
    sync_properties()
    sync_units()
    sync_owners()
    sync_tenants()
    sync_leases()
    sync_lease_payments()
    sync_work_orders()
    sync_vendors()
    sync_files()
    sync_notes()
    sync_tasks()
