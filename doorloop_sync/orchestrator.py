from doorloop_sync.tasks.raw_sync import (
    sync_accounts,
    sync_activity_logs,
    sync_applications,
    sync_communications,
    sync_files,
    sync_inspections,
    sync_insurance_policies,
    sync_lease_charges,
    sync_lease_credits,
    sync_lease_payments,
    sync_leases,
    sync_notes,
    sync_owners,
    sync_payments,
    sync_portfolios,
    sync_properties,
    sync_recurring_charges,
    sync_recurring_credits,
    sync_reports,
    sync_tasks,
    sync_tenants,
    sync_units,
    sync_users,
    sync_vendors,
)

from doorloop_sync.tasks.normalization import (
    normalize_accounts,
    normalize_activity_logs,
    normalize_applications,
    normalize_communications,
    normalize_files,
    normalize_inspections,
    normalize_insurance_policies,
    normalize_lease_charges,
    normalize_lease_credits,
    normalize_lease_payments,
    normalize_leases,
    normalize_notes,
    normalize_owners,
    normalize_payments,
    normalize_portfolios,
    normalize_properties,
    normalize_recurring_charges,
    normalize_recurring_credits,
    normalize_reports,
    normalize_tasks,
    normalize_tenants,
    normalize_units,
    normalize_users,
    normalize_vendors,
)


def run_task(task_func):
    try:
        print(f"🔄 Running {task_func.__module__}.{task_func.__name__} ...")
        task_func()
        print(f"✅ Completed {task_func.__module__}")
    except Exception as e:
        print(f"❌ Error in {task_func.__module__}: {e}")


def main():
    # Raw Sync Phase
    run_task(sync_accounts.run)
    run_task(sync_activity_logs.run)
    run_task(sync_applications.run)
    run_task(sync_communications.run)
    run_task(sync_files.run)
    run_task(sync_inspections.run)
    run_task(sync_insurance_policies.run)
    run_task(sync_lease_charges.run)
    run_task(sync_lease_credits.run)
    run_task(sync_lease_payments.run)
    run_task(sync_leases.run)
    run_task(sync_notes.run)
    run_task(sync_owners.run)
    run_task(sync_payments.run)
    run_task(sync_portfolios.run)
    run_task(sync_properties.run)
    run_task(sync_recurring_charges.run)
    run_task(sync_recurring_credits.run)
    run_task(sync_reports.run)
    run_task(sync_tasks.run)
    run_task(sync_tenants.run)
    run_task(sync_units.run)
    run_task(sync_users.run)
    run_task(sync_vendors.run)

    # Normalization Phase
    run_task(normalize_accounts.run)
    run_task(normalize_activity_logs.run)
    run_task(normalize_applications.run)
    run_task(normalize_communications.run)
    run_task(normalize_files.run)
    run_task(normalize_inspections.run)
    run_task(normalize_insurance_policies.run)
    run_task(normalize_lease_charges.run)
    run_task(normalize_lease_credits.run)
    run_task(normalize_lease_payments.run)
    run_task(normalize_leases.run)
    run_task(normalize_notes.run)
    run_task(normalize_owners.run)
    run_task(normalize_payments.run)
    run_task(normalize_portfolios.run)
    run_task(normalize_properties.run)
    run_task(normalize_recurring_charges.run)
    run_task(normalize_recurring_credits.run)
    run_task(normalize_reports.run)
    run_task(normalize_tasks.run)
    run_task(normalize_tenants.run)
    run_task(normalize_units.run)
    run_task(normalize_users.run)
    run_task(normalize_vendors.run)


if __name__ == "__main__":
    main()
