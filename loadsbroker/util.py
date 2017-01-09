"""Utility functions"""
from functools import wraps
import logging
import logging.handlers

from loadsbroker import logger


def set_logger(debug=False, name='loads', logfile='stdout'):
    """Setup the logger"""
    logger_ = logging.getLogger(name)
    logger.propagate = False

    if logfile == 'stdout':
        ch = logging.StreamHandler()
    else:
        ch = logging.handlers.RotatingFileHandler(logfile, mode='a+')

    if debug:
        ch.setLevel(logging.DEBUG)
        logger_.setLevel(logging.DEBUG)
    else:
        ch.setLevel(logging.INFO)
        logger_.setLevel(logging.INFO)

    formatter = logging.Formatter('[%(asctime)s][%(process)d] %(message)s')
    ch.setFormatter(formatter)
    logger_.addHandler(ch)


def retry(attempts=3,
          on_exception=None,
          on_result=None):
    """Retry a function multiple times, logging failures."""
    assert on_exception or on_result

    def __retry(func):
        @wraps(func)
        def ___retry(*args, **kw):
            attempt = 0
            while True:
                attempt += 1
                try:
                    result = func(*args, **kw)
                except Exception as exc:
                    if (on_exception is None or not on_exception(exc) or
                            attempt == attempts):
                        logger.debug('Failed (%d/%d)' % (attempt, attempts),
                                     exc_info=True)
                        raise
                else:
                    if (on_result is None or not on_result(result) or
                            attempt == attempts):
                        return result
        return ___retry
    return __retry


def join_host_port(host, port):
    """Joins a host and port"""
    if ":" in host or "%" in host:
        host = "[" + host + "]"

    return "%s:%d" % (host, port)
