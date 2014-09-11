import sys
import argparse

import requests

from loadsbroker import logger
from loadsbroker.util import set_logger


def _parse(sysargs=None):
    if sysargs is None:
        sysargs = sys.argv[1:]

    parser = argparse.ArgumentParser(description='Runs a Loads client.')

    parser.add_argument('--scheme', help='Server Scheme', type=str,
                        default='http')

    parser.add_argument('--host', help='Server Host', type=str,
                        default='localhost')
    parser.add_argument('--port', help='Server Port', type=int,
                        default=8080)

    parser.add_argument('--debug', help='Debug Info.', action='store_true',
                        default=True)

    parser.add_argument('command', help='Command to run',
                        choices=_COMMANDS.keys())
    args = parser.parse_args(sysargs)
    return args, parser


# XXX auto register with ABC ?
from loadsbroker.client.run import Run
from loadsbroker.client.info import Info
from loadsbroker.client.status import Status
from loadsbroker.client.abort import Abort


_COMMANDS = {'run': Run, 'info': Info,
             'status': Status, 'abort': Abort}


class Client(object):

    def __init__(self, host='localhost', port=8080, scheme='http'):
        self.port = port
        self.host = host
        self.scheme = scheme
        self.root = '%s://%s:%d' % (scheme, host, port)
        self.session = requests.Session()

    def __call__(self, command, **options):
        cmd = _COMMANDS[command]
        return cmd(self.session, self.root)(**options)


def main(sysargs=None):
    args, parser = _parse(sysargs, _COMMANDS.keys())
    set_logger(debug=args.debug)

    c = Client(args.host, args.port, args.scheme)

    try:
        print(c(args.command))
    except requests.exceptions.ConnectionError as e:
        logger.debug('Cannot connect => ' + str(e))


if __name__ == '__main__':
    main()
