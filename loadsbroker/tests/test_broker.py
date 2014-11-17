import os

from tornado.testing import AsyncTestCase, gen_test

from loadsbroker.broker import RunManager, RunHelpers
from loadsbroker.db import Database, Strategy, ContainerSet
from loadsbroker.aws import EC2Pool
from loadsbroker.tests.util import (start_moto, create_images,
                                    start_docker, start_influx)


endpoints = os.path.join(os.path.dirname(__file__),
                         'endpoints.json')
os.environ['BOTO_ENDPOINTS'] = endpoints


class TestRunManager(AsyncTestCase):

    @classmethod
    def setUpClass(cls):
        try:
            cls.docker = start_docker()
        except Exception:
            cls.docker = None
            raise

        try:
            cls.influx = start_influx()
        except Exception:
            cls.influx = None
            raise

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
        if cls.docker is not None:
            cls.docker.kill()
        if cls.influx is not None:
            cls.influx.kill()

    def setUp(self):
        super(TestRunManager, self).setUp()
        self.db = Database('sqlite:///:memory:')
        self.session = self.db.session()
        self.pool = EC2Pool('mybroker', access_key='key',
                            secret_key='xxx', use_filters=False, owner_id=None,
                            port=5000)

    def tearDown(self):
        self.pool.shutdown()
        super(TestRunManager, self).tearDown()

    @gen_test
    def test_run(self):
        # the first thing to do is to create a container set and a strategy
        url = "https://s3.amazonaws.com/loads-images/simpletest-dev.tar.gz"

        cs = ContainerSet(
            name='yeah',
            instance_count=1,
            container_name="bbangert/simpletest:dev",
            container_url=url)

        strategy = Strategy(name='strategic!', uuid='strategic!',
                            container_sets=[cs])

        self.session.add(strategy)
        self.session.commit()

        # now we can start a new run
        run_helpers = RunHelpers()

        mgr, future = RunManager.new_run(run_helpers, self.session, self.pool,
                                         self.io_loop, 'strategic!')

        response = yield future
        self.assertEqual(response, True)
