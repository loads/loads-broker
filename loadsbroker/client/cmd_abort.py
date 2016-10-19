from loadsbroker.client.base import BaseCommand


class Abort(BaseCommand):
    """Abort a run."""

    name = 'abort'
    arguments = {'run_id': {'help': 'Run Id'},
                 '--terminate': {'help': '', 'default': False,
                                 'action': 'store_true'}}

    def __call__(self, args):
        url = '/run/' + args.run_id
        if args.terminate:
            url += '?terminate=1'
        return self.session.delete(self.root + url).json()


cmd = Abort
