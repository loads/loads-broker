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
                 '--cset-name': {'help': 'Name of container set'}}

    def __call__(self, args):
        options = json.dumps(self.args2options(args))
        headers = {'Content-Type': 'application/json'}
        r = self.session.post(self.root, data=options, headers=headers)
        return r.json()

cmd = Run
