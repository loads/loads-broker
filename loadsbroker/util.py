from functools import wraps
from loadsbroker import logger


def retry(attempts=3):
    def __retry(func):
        @wraps(func)
        def ___retry(*args, **kw):
            attempt = 1
            while attempt < attempts:
                try:
                    return func(*args, **kw)
                except Exception:
                    logger.debug('Failed (%d/%d)' % (attempt, attempts),
                                 exc_info=True)
                    attempt += 1
            # failed
            raise
        return ___retry
    return __retry
