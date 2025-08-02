from doorloop_sync.tasks.raw_sync import sync_properties, sync_units
from doorloop_sync.tasks.normalization import normalize_properties, normalize_units

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

    # === NORMALIZATION ===
    run_task(normalize_properties.run)
    run_task(normalize_units.run)

if __name__ == "__main__":
    main()