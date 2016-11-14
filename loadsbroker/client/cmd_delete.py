from loadsbroker.client.cmd_abort import Abort


class Delete(Abort):
    """Delete a run."""
    name = 'delete'

    def __call__(self, args):
        url = '/run/' + args.run_id + '?purge=1'
        return self.session.delete(self.root + url).json()


cmd = Delete
