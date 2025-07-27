# normalize_and_patch_all.py

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

    normalizers = [
        ("Properties", normalize_properties),
        ("Units", normalize_units),
        ("Leases", normalize_leases),
        ("Tenants", normalize_tenants),
        ("Owners", normalize_owners),
        ("Payments", normalize_payments),
        ("Lease Charges", normalize_lease_charges),
        ("Lease Credits", normalize_lease_credits),
        ("Work Orders", normalize_work_orders),
        ("Vendors", normalize_vendors),
        ("Tasks", normalize_tasks),
        ("Users", normalize_users),
        ("GL Accounts", normalize_gl_accounts),
        ("Bank Accounts", normalize_bank_accounts),
        ("Property Settings", normalize_property_settings),
    ]

    for label, fn in normalizers:
        try:
            print(f"➡️ Running {label} normalization...")
            fn()
            print(f"✅ {label} normalization complete.")
        except Exception as e:
            print(f"❌ ERROR during {label} normalization:")
            print(e)

    print("🎉 All normalization steps finished.")

if __name__ == "__main__":
    run_all_normalizations()
