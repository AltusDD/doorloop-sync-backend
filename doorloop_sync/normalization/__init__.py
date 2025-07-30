from .normalize_properties import run as normalize_properties
from .normalize_units import run as normalize_units
from .normalize_leases import run as normalize_leases
from .normalize_tenants import run as normalize_tenants
from .normalize_owners import run as normalize_owners
from .normalize_tasks import run as normalize_tasks
from .normalize_vendors import run as normalize_vendors
from .normalize_lease_payments import run as normalize_lease_payments

def run_all_normalizations(supabase_client):
    print("🚧 Starting normalization pipeline...")
    try:
        normalize_properties(supabase_client)
        print("✅ Properties normalized")
    except Exception as e:
        print(f"❌ Failed to normalize properties: {e}")

    try:
        normalize_units(supabase_client)
        print("✅ Units normalized")
    except Exception as e:
        print(f"❌ Failed to normalize units: {e}")

    try:
        normalize_leases(supabase_client)
        print("✅ Leases normalized")
    except Exception as e:
        print(f"❌ Failed to normalize leases: {e}")

    try:
        normalize_tenants(supabase_client)
        print("✅ Tenants normalized")
    except Exception as e:
        print(f"❌ Failed to normalize tenants: {e}")

    try:
        normalize_owners(supabase_client)
        print("✅ Owners normalized")
    except Exception as e:
        print(f"❌ Failed to normalize owners: {e}")

    try:
        normalize_tasks(supabase_client)
        print("✅ Tasks normalized")
    except Exception as e:
        print(f"❌ Failed to normalize tasks: {e}")

    try:
        normalize_vendors(supabase_client)
        print("✅ Vendors normalized")
    except Exception as e:
        print(f"❌ Failed to normalize vendors: {e}")

    try:
        normalize_lease_payments(supabase_client)
        print("✅ Lease payments normalized")
    except Exception as e:
        print(f"❌ Failed to normalize lease payments: {e}")

    print("🏁 Normalization pipeline completed.")