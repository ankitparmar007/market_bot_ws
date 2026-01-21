import time
import inspect
from functools import wraps


def timing_decorator(func):
    if inspect.iscoroutinefunction(func):
        # Async version
        @wraps(func)
        async def async_wrapper(*args, **kwargs):
            start = time.time()
            result = await func(*args, **kwargs)
            end = time.time()
            elapsed_ms = (end - start) * 1000
            print(f"{func.__name__} took {elapsed_ms:.2f} ms")
            return result

        return async_wrapper
    else:
        # Sync version
        @wraps(func)
        def sync_wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            end = time.time()
            elapsed_ms = (end - start) * 1000
            print(f"{func.__name__} took {elapsed_ms:.2f} ms")
            return result

        return sync_wrapper
