import sys
import argparse
import os

import tornado.ioloop

from loadsbroker.util import set_logger
from loadsbroker.broker import Broker
from loadsbroker.webapp import application
from loadsbroker.options import InfluxOptions, HekaOptions
from loadsbroker import logger


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
    parser.add_argument('--aws-port', help='AWS Port', type=int, default=None)
    parser.add_argument('--aws-endpoints', help='AWS Endpoints', type=str,
                        default=None)
    parser.add_argument('--aws-owner-id', help='AWS Owner ID', type=str,
                        default="595879546273")
    parser.add_argument('--aws-skip-filters', help='Use AWS filters',
                        action='store_true', default=False)
    parser.add_argument('--heka-host', help='Heka host', type=str,
                        default='172.31.34.9')
    parser.add_argument('--heka-port', help='Heka port', type=int,
                        default=6745)
    parser.add_argument('--heka-secure', help='Use TLS for Heka',
                        action='store_true', default=False)
    parser.add_argument('--influx-host', help='InfluxDB host', type=str,
                        default='localhost')
    parser.add_argument('--influx-port', help='InfluxDB port', type=int,
                        default=8086)
    parser.add_argument('--influx-user', help='InfluxDB username', type=str,
                        default='root')
    parser.add_argument('--influx-password', help='InfluxDB password',
                        type=str, default='root')
    parser.add_argument('--influx-secure', help='Use TLS for InfluxDB',
                        action='store_true', default=False)

    args = parser.parse_args(sysargs)
    return args, parser


def main(sysargs=None):
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

    heka_options = HekaOptions(args.heka_host, args.heka_port,
                               args.heka_secure)

    influx_options = InfluxOptions(args.influx_host, args.influx_port,
                                   args.influx_user, args.influx_password,
                                   args.influx_secure)

    application.broker = Broker(loop, args.database, args.ssh_key,
                                args.ssh_username,
                                heka_options,
                                influx_options,
                                aws_port=args.aws_port,
                                aws_owner_id=aws_owner_id,
                                aws_use_filters=not args.aws_skip_filters,
                                aws_access_key=aws_access_key,
                                aws_secret_key=aws_secret_key)

    logger.debug('Listening on port %d...' % args.port)
    application.listen(args.port)
    try:
        loop.start()
    except KeyboardInterrupt:
        logger.debug('Bye')


if __name__ == '__main__':
    main()
