# doorloop_sync/orchestrator.py

from doorloop_sync.tasks.raw_sync import (
    sync_properties,
    sync_units,
    sync_leases,
    sync_tenants,
    sync_owners,
    sync_payments,
)

from doorloop_sync.tasks.normalization import (
    normalize_properties,
    normalize_units,
    normalize_leases,
    normalize_tenants,
    normalize_owners,
    normalize_payments,
)

def run_task(task_func):
    try:
        print(f"🔄 Running {task_func.__module__}.{task_func.__name__} ...")
        task_func()
        print(f"✅ Completed {task_func.__module__}")
    except Exception as e:
        print(f"❌ Error in {task_func.__module__}: {e}")

def main():
    # === RAW SYNC ===
    run_task(sync_properties.run)
    run_task(sync_units.run)
    run_task(sync_leases.run)
    run_task(sync_tenants.run)
    run_task(sync_owners.run)
    run_task(sync_payments.run)

    # === NORMALIZATION ===
    run_task(normalize_properties.run)
    run_task(normalize_units.run)
    run_task(normalize_leases.run)
    run_task(normalize_tenants.run)
    run_task(normalize_owners.run)
    run_task(normalize_payments.run)

if __name__ == "__main__":
    main()
