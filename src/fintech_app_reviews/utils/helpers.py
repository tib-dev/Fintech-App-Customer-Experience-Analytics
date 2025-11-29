import logging
from functools import wraps

logger = logging.getLogger(__name__)


def safe_run(default=None):
    """
    Decorator: catch errors inside a function and return a fallback value.
    """
    def wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"{func.__name__} failed: {e}", exc_info=True)
                return default
        return inner
    return wrapper
