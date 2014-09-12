from loadsbroker.client.base import BaseCommand


class Info(BaseCommand):
    """Returns general info about the Broker."""
    name = 'info'


cmd = Info
