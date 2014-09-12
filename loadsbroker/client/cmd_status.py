from loadsbroker.client.base import BaseCommand


class Status(BaseCommand):
    name = 'status'
    arguments = {'run_id': {'help': 'Run Id'}}

    def __call__(self, args):
        url = self.root + '/run/' + args.run_id
        return self.session.get(url).json()

cmd = Status
