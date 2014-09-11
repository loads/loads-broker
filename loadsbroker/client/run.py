import json
from loadsbroker.client.base import BaseCommand


class Run(BaseCommand):
    name = 'run'

    def __call__(self, **options):
        options = json.dumps(options)
        headers = {'Content-Type': 'application/json'}
        r = self.session.post(self.root, data=options, headers=headers)
        return r.json()
