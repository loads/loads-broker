import sys
import argparse
import os
import json

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

    subparsers = parser.add_subparsers(help='sub-command help')

    for cmd_name, cmd in sorted(_COMMANDS.items()):

        sub = subparsers.add_parser(cmd_name, help=cmd.__doc__.strip())
        for argument, options in cmd.arguments.items():
            sub.add_argument(argument, **options)

        sub.set_defaults(func=cmd)

    args = parser.parse_args(sysargs)
    return args, parser


_COMMANDS = {}


def load_commands():
    for file in os.listdir(os.path.dirname(__file__)):
        if file.startswith('cmd_') and file.endswith('.py'):
            mod = 'loadsbroker.client.' + file[:-len('.py')]
            mod = __import__(mod, globals(), locals(), ['cmd'], 0)
            _COMMANDS[mod.cmd.name] = mod.cmd


load_commands()


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
    args, parser = _parse(sysargs)
    set_logger(debug=args.debug)

    c = Client(args.host, args.port, args.scheme)

    if not hasattr(args, 'func'):
        args.func = _COMMANDS['info']

    args.func = args.func(c.session, c.root)

    try:
        res = args.func(args)
        print(json.dumps(res))
    except requests.exceptions.ConnectionError as e:
        logger.debug('Cannot connect => ' + str(e))


if __name__ == '__main__':
    main()
