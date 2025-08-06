
from doorloop_sync.tasks.raw_sync.sync_properties import sync_properties
from doorloop_sync.tasks.raw_sync.sync_units import sync_units
from doorloop_sync.tasks.raw_sync.sync_owners import sync_owners
from doorloop_sync.tasks.raw_sync.sync_tenants import sync_tenants
from doorloop_sync.tasks.raw_sync.sync_leases import sync_leases
from doorloop_sync.tasks.raw_sync.sync_lease_payments import sync_lease_payments
from doorloop_sync.tasks.raw_sync.sync_work_orders import sync_work_orders
from doorloop_sync.tasks.raw_sync.sync_vendors import sync_vendors
from doorloop_sync.tasks.raw_sync.sync_files import sync_files
from doorloop_sync.tasks.raw_sync.sync_notes import sync_notes
from doorloop_sync.tasks.raw_sync.sync_tasks import sync_tasks

def sync_all():
    print("🚀 Starting sync for: properties")
    try:
        sync_properties()
    except Exception as e:
        print(f"❌ ERROR during sync for properties: {e}")

    print("🚀 Starting sync for: units")
    try:
        sync_units()
    except Exception as e:
        print(f"❌ ERROR during sync for units: {e}")

    print("🚀 Starting sync for: owners")
    try:
        sync_owners()
    except Exception as e:
        print(f"❌ ERROR during sync for owners: {e}")

    print("🚀 Starting sync for: tenants")
    try:
        sync_tenants()
    except Exception as e:
        print(f"❌ ERROR during sync for tenants: {e}")

    print("🚀 Starting sync for: leases")
    try:
        sync_leases()
    except Exception as e:
        print(f"❌ ERROR during sync for leases: {e}")

    print("🚀 Starting sync for: lease_payments")
    try:
        sync_lease_payments()
    except Exception as e:
        print(f"❌ ERROR during sync for lease_payments: {e}")

    print("🚀 Starting sync for: work_orders")
    try:
        sync_work_orders()
    except Exception as e:
        print(f"❌ ERROR during sync for work_orders: {e}")

    print("🚀 Starting sync for: vendors")
    try:
        sync_vendors()
    except Exception as e:
        print(f"❌ ERROR during sync for vendors: {e}")

    print("🚀 Starting sync for: files")
    try:
        sync_files()
    except Exception as e:
        print(f"❌ ERROR during sync for files: {e}")

    print("🚀 Starting sync for: notes")
    try:
        sync_notes()
    except Exception as e:
        print(f"❌ ERROR during sync for notes: {e}")

    print("🚀 Starting sync for: tasks")
    try:
        sync_tasks()
    except Exception as e:
        print(f"❌ ERROR during sync for tasks: {e}")
