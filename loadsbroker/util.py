from functools import wraps
import logging
import logging.handlers

from tornado.concurrent import Future

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


"""Restrains a function that shouldn't be run concurrently to wait till
other concurrent calls have finished before this call runs.

The function must be underneath a coroutine decorator, ie:

.. code-block:: python

    @gen.coroutine
    @non_concurrent
    def my_func():
        # Do something

"""
def non_concurrent(func):
    func._running = None

    @wraps(func)
    def _run_once(*args, **kw):
        # repeat as long as we have a future
        while func._running:
            yield func._running

        func._running = Future()

        try:
            return func(*args, **kw)
        finally:
            func._running.set_result(True)
            func._running = None
    return _run_once


"""Restrains a function to run only once per stringable keyword arg of the
function.

This behaves similarly to @non_concurrent except that concurrent calls with
different values for the supplied keyword arguments will be allowed.

"""
def non_concurrent_on(*keyword_args):
    if not keyword_args:
        raise Exception("Missing keyword_args.")

    def _nco(func):
        func._running = {}

        @wraps(func)
        def __nco(*args, **kw):
            key = "".join(kw[x] for x in keyword_args)

            # repeat as long as we have a future
            while func._running.get(key):
                yield func._running[key]

            func._running[key] = Future()

            try:
                return func(*args, **kw)
            finally:
                func._running[key].set_result(True)
                func._running[key] = None

        return __nco
    return _nco
