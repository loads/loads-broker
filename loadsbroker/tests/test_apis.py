import os
import json

from tornado.testing import AsyncHTTPTestCase

from loadsbroker import __version__
from loadsbroker.api import application
from loadsbroker.broker import Broker
from loadsbroker.tests.util import start_moto, create_images


class TestAPI(AsyncHTTPTestCase):

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
        self.broker = None
        super(TestAPI, self).setUp()

    def tearDown(self):
        if self.broker is not None:
            self.broker.shutdown()
        super(TestAPI, self).tearDown()

    def get_app(self):
        try:
            endpoints = os.path.join(os.path.dirname(__file__),
                                     'endpoints.json')

            os.environ['BOTO_ENDPOINTS'] = endpoints

            self.broker = Broker(
                self.io_loop, 'sqlite:////tmp/loads.db',
                '', 'core', 5000,
                aws_owner_id=None,
                aws_use_filters=False)

            application.broker = self.broker
            return application
        except Exception:
            self.tearDownClass()

    def test_root(self):
        response = self.fetch('/')
        self.assertTrue(response.code, 200)

        body = json.loads(response.body.decode())
        self.assertEqual(body['version'], __version__)
