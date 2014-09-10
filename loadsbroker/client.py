import sys
import argparse

import requests

from loadsbroker import logger
from loadsbroker.util import set_logger


def _parse(sysargs=None, commands=None):
    if commands is None:
        commands = []
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

    parser.add_argument('command', help='Command to run', choices=commands)
    args = parser.parse_args(sysargs)
    return args, parser


class Client(object):
    commands = ['info']

    def __init__(self, host='localhost', port=8080, scheme='http'):
        self.port = port
        self.host = host
        self.scheme = scheme
        self.root = '%s://%s:%d' % (scheme, host, port)
        self.session = requests.Session()

    def __call__(self, command, *args, **kw):
        return getattr(self, 'cmd_' + command)(*args, **kw)

    def cmd_info(self):
        return self.session.get(self.root).json()


def main(sysargs=None):
    args, parser = _parse(sysargs, Client.commands)
    set_logger(debug=args.debug)

    c = Client(args.host, args.port, args.scheme)

    try:
        print(c(args.command))
    except requests.exceptions.ConnectionError as e:
        logger.debug('Cannot connect => ' + str(e))


if __name__ == '__main__':
    main()
