from loadsbroker.client.base import BaseCommand


class Status(BaseCommand):
    name = 'status'

    def __call__(self, **options):
        url = self.root + '/run/' + options['run_id']
        return self.session.get(url).json()
