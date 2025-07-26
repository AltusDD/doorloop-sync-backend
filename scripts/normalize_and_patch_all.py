import sys
import os

# 🚨 Empire-grade path resolution
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from sync_pipeline.normalize_properties import normalize_properties
from sync_pipeline.normalize_units import normalize_units
from sync_pipeline.normalize_leases import normalize_leases
from sync_pipeline.normalize_tenants import normalize_tenants
from sync_pipeline.normalize_owners import normalize_owners
from sync_pipeline.normalize_payments import normalize_payments
from sync_pipeline.normalize_lease_charges import normalize_lease_charges
from sync_pipeline.normalize_lease_credits import normalize_lease_credits
from sync_pipeline.normalize_work_orders import normalize_work_orders
from sync_pipeline.normalize_vendors import normalize_vendors
from sync_pipeline.normalize_tasks import normalize_tasks
from sync_pipeline.normalize_users import normalize_users
from sync_pipeline.normalize_gl_accounts import normalize_gl_accounts
from sync_pipeline.normalize_bank_accounts import normalize_bank_accounts
from sync_pipeline.normalize_property_settings import normalize_property_settings

def run_all_normalizations():
    print("🔄 Starting normalization process...")

    normalize_properties()
    normalize_units()
    normalize_leases()
    normalize_tenants()
    normalize_owners()
    normalize_payments()
    normalize_lease_charges()
    print("📦 Phase 1 normalization complete.")

    normalize_lease_credits()
    normalize_work_orders()
    normalize_vendors()
    normalize_tasks()
    normalize_users()
    normalize_gl_accounts()
    normalize_bank_accounts()
    normalize_property_settings()
    print("✅ Phase 2 normalization complete.")

if __name__ == "__main__":
    run_all_normalizations()
