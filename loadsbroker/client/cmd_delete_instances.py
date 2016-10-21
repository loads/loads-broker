from loadsbroker.client.base import BaseCommand


class TerminateInstances(BaseCommand):
    """Terminate instances"""
    name = 'terminate_all'

    def __call__(self, args):
        url = self.root + '/instances'
        res = self.session.delete(url)
        return res.json()


cmd = TerminateInstances
