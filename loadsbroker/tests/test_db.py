import unittest
from loadsbroker.db import Project, Strategy, Collection, Run, Database


class DatabaseTest(unittest.TestCase):

    def setUp(self):
        self.db = Database('sqlite:///:memory:')

    def test_project(self):
        session = self.db.session()

        # a project is defined by a name, a repo and strategies
        project = Project(
            name='simplepush',
            repository='https://github.com/mozilla-services/pushgo',
            home_page='https://services.mozilla.com')

        session.add(project)

        strategy = Strategy(name='s1', enabled=True, trigger_url='wat?')
        project.strategies = [strategy]

        run = Run(uuid='yeah')
        strategy.runs = [run]

        # Collection is a confusing name.
        # also confused by what's a run_order
        collection = Collection(name='collection')
        strategy.collections = [collection]
        session.commit()
