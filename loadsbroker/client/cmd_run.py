import json
from loadsbroker.client.base import BaseCommand


class Run(BaseCommand):
    """Launches a Load test.
    """
    name = 'run'
    arguments = {'--nodes': {'help': 'Number of nodes to instanciate',
                             'default': 1, 'type': int},
                 '--user-data': {'help': 'user-data', 'default': None,
                                 'type': str},
                 '--image-url': {'help': 'URL of image to use'},
                 '--image-name': {'help': 'Name of image to use'},
                 '--cset-name': {'help': 'Name of container set'},
                 '--strategy-id': {'help': 'Strategy ID to use',
                                   'default': 'strategic!'}}

    def __call__(self, args):
        options = self.args2options(args)
        headers = {'Content-Type': 'application/json'}
        strategy_id = options['strategy_id']
        r = self.session.post(self.root + '/orchestrate/%s' % strategy_id,
                              data=json.dumps(options), headers=headers)
        return r.json()


cmd = Run
