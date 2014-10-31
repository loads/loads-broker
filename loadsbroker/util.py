from functools import wraps
import logging
import logging.handlers

from loadsbroker import logger


def set_logger(debug=False, name='loads', logfile='stdout'):
    logger_ = logging.getLogger(name)
    logger_.setLevel(logging.DEBUG)
    logger.propagate = False

    if logfile == 'stdout':
        ch = logging.StreamHandler()
    else:
        ch = logging.handlers.RotatingFileHandler(logfile, mode='a+')

    if debug:
        ch.setLevel(logging.DEBUG)
    else:
        ch.setLevel(logging.INFO)

    formatter = logging.Formatter('[%(asctime)s][%(process)d] %(message)s')
    ch.setFormatter(formatter)
    logger_.addHandler(ch)


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


def dict2str(data):
    data = ['%s=%s' % (key, str(val)) for key, val in data.items()]
    return '\n'.join(data)
