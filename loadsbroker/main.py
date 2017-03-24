"""loads-broker daemon script"""
import sys
import argparse
import os

import tornado.ioloop

from loadsbroker.util import set_logger
from loadsbroker.broker import Broker
from loadsbroker.webapp import application
from loadsbroker import logger


def _parse(sysargs=None):
    if sysargs is None:
        sysargs = sys.argv[1:]

    parser = argparse.ArgumentParser(description='Runs a Loads broker.')
    parser.add_argument('--name', help="Name of this broker instance",
                        type=str, default="1234")
    parser.add_argument('-p', '--port', help='HTTP Port', type=int,
                        default=8080)
    parser.add_argument('--debug', help='Debug Info.', action='store_true',
                        default=False)
    parser.add_argument('-d', '--database', help='URI of database', type=str,
                        default='sqlite:////tmp/loads.db')
    parser.add_argument('-k', '--ssh-key', help='SSH PEM file', type=str)
    parser.add_argument('--aws-port', help='AWS Port', type=int, default=None)
    parser.add_argument('--aws-endpoints', help='AWS Endpoints', type=str,
                        default=None)
    parser.add_argument('--aws-owner-id', help='AWS Owner ID', type=str,
                        default="595879546273")
    parser.add_argument('--aws-skip-filters', help='Use AWS filters',
                        action='store_true', default=False)
    # XXX: deprecate
    parser.add_argument('--no-influx', help='Deactivate Influx.',
                        action='store_true', default=False)
    parser.add_argument('--initial-db', help="JSON file to initialize the db.",
                        type=str, default=os.path.join(
                            os.path.dirname(__file__), '..', 'pushgo.json'))
    args = parser.parse_args(sysargs)
    return args, parser


def main(sysargs=None):
    """Parses arguments and starts up the loads-broker.

    This daemon runs in the foreground.

    """
    args, parser = _parse(sysargs)
    set_logger(debug=args.debug)
    loop = tornado.ioloop.IOLoop.instance()

    if args.aws_endpoints is not None:
        os.environ['BOTO_ENDPOINTS'] = args.aws_endpoints

    # an empty string means we don't filter by owner id
    # we translate this to None
    aws_owner_id = args.aws_owner_id and args.aws_owner_id or None
    aws_access_key = os.environ.get('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.environ.get('AWS_SECRET_ACCESS_KEY')

    application.broker = Broker(args.name, loop, args.database, args.ssh_key,
                                aws_port=args.aws_port,
                                aws_owner_id=aws_owner_id,
                                aws_use_filters=not args.aws_skip_filters,
                                aws_access_key=aws_access_key,
                                aws_secret_key=aws_secret_key,
                                initial_db=args.initial_db)

    logger.info('Listening on port %d...' % args.port)
    application.listen(args.port)
    try:
        loop.start()
    except KeyboardInterrupt:
        logger.info('Bye')


if __name__ == '__main__':
    main()
