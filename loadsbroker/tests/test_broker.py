import os

from tornado.testing import AsyncTestCase, gen_test

from loadsbroker.broker import RunManager
from loadsbroker.db import Database, Strategy, ContainerSet
from loadsbroker.aws import EC2Pool
from loadsbroker.tests.util import start_moto, create_images


endpoints = os.path.join(os.path.dirname(__file__),
                         'endpoints.json')
os.environ['BOTO_ENDPOINTS'] = endpoints


class TestRunManager(AsyncTestCase):

    @classmethod
    def setUpClass(cls):
        cls.moto = None
        try:
            cls.moto = start_moto()
            create_images()
        except Exception:
            if cls.moto is None:
                cls.moto.kill()
            raise

    @classmethod
    def tearDownClass(cls):
        if cls.moto is not None:
            cls.moto.kill()

    def setUp(self):
        super(TestRunManager, self).setUp()
        self.db = Database('sqlite:///:memory:')
        self.session = self.db.session()
        self.pool = EC2Pool('mybroker', use_filters=False, owner_id=None,
                            port=5000)

    def tearDown(self):
        self.pool.shutdown()
        super(TestRunManager, self).tearDown()

    @gen_test
    def test_run(self):
        # the first thing to do is to create a container set and a strategy
        strategy = Strategy(name='strategic!',
                            container_sets=[ContainerSet(name='yeah')])
        self.session.add(strategy)
        self.session.commit()

        # now we can start a new run
        mgr, future = RunManager.new_run(self.session, self.pool,
                                         self.io_loop, 'strategic!')

        yield future
