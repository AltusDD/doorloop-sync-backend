from functools import wraps

def task_error_handler(func=None, *, task_name=None):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            try:
                return fn(*args, **kwargs)
            except Exception as e:
                name = task_name or fn.__name__
                print(f"❌ Error syncing {name}: {e}")
                return None
        return wrapper

    if func is None:
        return decorator
    return decorator(func)
