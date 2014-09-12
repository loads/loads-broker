import json
from loadsbroker.client.base import BaseCommand


class Run(BaseCommand):
    """Launches a Load test.
    """
    name = 'run'
    arguments = {'--ami': {'help': 'AMI to use',
                           'default': 'ami-3193e801'},
                 '--nodes': {'help': 'Number of nodes to instanciate',
                             'default': 1, 'type': int},
                 '--user-data': {'help': 'user-data', 'default': None,
                                 'type': str}}

    def __call__(self, args):
        options = json.dumps(self.args2options(args))
        headers = {'Content-Type': 'application/json'}
        r = self.session.post(self.root, data=options, headers=headers)
        return r.json()

cmd = Run
