from loadsbroker.client.base import BaseCommand


class Abort(BaseCommand):
    name = 'abort'

    def __call__(self, **options):
        url = self.root + '/run/' + options['run_id']
        return self.session.delete(url).json()
