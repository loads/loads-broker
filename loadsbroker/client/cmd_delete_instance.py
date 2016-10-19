from loadsbroker.client.base import BaseCommand


class TerminateInstance(BaseCommand):
    """Terminate instances"""
    name = 'terminate'
    arguments = {'instance_id': {'help': 'Instance Id'}}

    def __call__(self, args):
        url = self.root + '/instances/' + args.instance_id
        res = self.session.delete(url)
        return res.json()


cmd = TerminateInstance
