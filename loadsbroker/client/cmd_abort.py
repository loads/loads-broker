from loadsbroker.client.base import BaseCommand


class Abort(BaseCommand):
    """Abort a run."""

    name = 'abort'
    arguments = {'run_id': {'help': 'Run Id'}}

    def __call__(self, args):
        url = '/run/' + args.run_id
        return self.session.delete(self.root + url).json()


cmd = Abort
