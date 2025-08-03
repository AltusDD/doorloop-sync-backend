# silent-tag: orchestrator-fix-0803
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), "doorloop_sync"))

from doorloop_sync.tasks.raw_sync import (
    sync_accounts, sync_activity_logs, sync_applications, sync_communications,
    sync_files, sync_inspections, sync_insurance_policies, sync_lease_charges,
    sync_lease_credits, sync_lease_payments, sync_leases, sync_notes,
    sync_owners, sync_payments, sync_portfolios, sync_properties,
    sync_recurring_charges, sync_recurring_credits, sync_reports, sync_tasks,
    sync_tenants, sync_units, sync_users, sync_vendors
)
from doorloop_sync.tasks.normalization import (
    normalize_accounts, normalize_activity_logs, normalize_applications,
    normalize_communications, normalize_files, normalize_inspections,
    normalize_insurance_policies, normalize_lease_charges,
    normalize_lease_credits, normalize_lease_payments, normalize_leases,
    normalize_notes, normalize_owners, normalize_payments,
    normalize_portfolios, normalize_properties, normalize_recurring_charges,
    normalize_recurring_credits, normalize_reports, normalize_tasks,
    normalize_tenants, normalize_units, normalize_users, normalize_vendors
)

if __name__ == "__main__":
    sync_accounts.run()
    sync_activity_logs.run()
    sync_applications.run()
    sync_communications.run()
    sync_files.run()
    sync_inspections.run()
    sync_insurance_policies.run()
    sync_lease_charges.run()
    sync_lease_credits.run()
    sync_lease_payments.run()
    sync_leases.run()
    sync_notes.run()
    sync_owners.run()
    sync_payments.run()
    sync_portfolios.run()
    sync_properties.run()
    sync_recurring_charges.run()
    sync_recurring_credits.run()
    sync_reports.run()
    sync_tasks.run()
    sync_tenants.run()
    sync_units.run()
    sync_users.run()
    sync_vendors.run()

    normalize_accounts.run()
    normalize_activity_logs.run()
    normalize_applications.run()
    normalize_communications.run()
    normalize_files.run()
    normalize_inspections.run()
    normalize_insurance_policies.run()
    normalize_lease_charges.run()
    normalize_lease_credits.run()
    normalize_lease_payments.run()
    normalize_leases.run()
    normalize_notes.run()
    normalize_owners.run()
    normalize_payments.run()
    normalize_portfolios.run()
    normalize_properties.run()
    normalize_recurring_charges.run()
    normalize_recurring_credits.run()
    normalize_reports.run()
    normalize_tasks.run()
    normalize_tenants.run()
    normalize_units.run()
    normalize_users.run()
    normalize_vendors.run()
