import sys
import argparse
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


def _parse(sysargs=None):
    if sysargs is None:
        sysargs = sys.argv[1:]

    parser = argparse.ArgumentParser(description='Runs a Loads broker.')
    parser.add_argument('-p', '--port', help='HTTP Port', type=int,
                        default=8080)
    parser.add_argument('--debug', help='Debug Info.', action='store_true',
                        default=True)
    parser.add_argument('-d', '--database', help='URI of database', type=str,
                        default='sqlite:////tmp/loads.db')
    parser.add_argument('-k', '--ssh-key', help='SSH PEM file', type=str,
                        default='/Users/tarek/.ssh/loads.pem')
    parser.add_argument('-u', '--ssh-username', help='SSH Username', type=str,
                        default='core')

    args = parser.parse_args(sysargs)
    return args, parser


def main(sysargs=None):
    args, parser = _parse(sysargs)
    _set_logger(debug=args.debug)
    loop = tornado.ioloop.IOLoop.instance()
    application.broker = Broker(loop, args.database, args.ssh_key,
                                args.ssh_username)
    logger.debug('Listening on port %d...' % args.port)
    application.listen(args.port)
    try:
        loop.start()
    except KeyboardInterrupt:
        logger.debug('Bye')


if __name__ == '__main__':
    main()
