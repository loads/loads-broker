from loadsbroker.client.base import BaseCommand


class Instances(BaseCommand):
    """Returns general info about running instances."""
    name = 'instances'

    def __call__(self, args):
        url = '/instances'
        return self.session.get(self.root + url).json()


cmd = Instances
