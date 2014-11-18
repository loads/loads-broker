from loadsbroker.client.base import BaseCommand


class Status(BaseCommand):
    """Display a status of a given run.
    """
    name = 'status'
    arguments = {'run_id': {'help': 'Run Id'}}

    def __call__(self, args):
        url = self.root + '/run/' + args.run_id
        res = self.session.get(url)
        return res.json()

cmd = Status
