from loadsbroker.client.base import BaseCommand


class Instance(BaseCommand):
    """Returns general info about running instances."""
    name = 'instance'
    arguments = {'instance_id': {'help': 'Instance Id'}}

    def __call__(self, args):
        url = self.root + '/instances/' + args.instance_id
        res = self.session.get(url)
        return res.json()


cmd = Instance
