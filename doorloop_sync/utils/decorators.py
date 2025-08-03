def task_error_handler(func):
    def wrapper(*args, **kwargs):
        task_name = func.__name__
        try:
            return func(*args, **kwargs)
        except Exception as e:
            print(f"❌ Error syncing {task_name.replace('_', ' ').title()}: {e}")
            return None
    return wrapper