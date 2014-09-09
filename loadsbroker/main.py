import logging
import logging.handlers

import tornado.ioloop

from loadsbroker.broker import Broker
from loadsbroker.api import application
from loadsbroker import logger


def _set_logger(debug=False, name='loads', logfile='stdout'):
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


def main():
    _set_logger(debug=True)
    loop = tornado.ioloop.IOLoop.instance()
    application.broker = Broker(io_loop=loop)
    logger.debug('Listening on port 8080...')
    application.listen(8080)
    loop.start()


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        pass
