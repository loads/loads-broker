import os
import json

from tornado.testing import AsyncHTTPTestCase

from loadsbroker import __version__
from loadsbroker.webapp import application
from loadsbroker.broker import Broker
from loadsbroker.tests.util import start_moto, create_images, start_influx
from loadsbroker.options import InfluxOptions


class TestAPI(AsyncHTTPTestCase):

    @classmethod
    def setUpClass(cls):
        cls.moto = cls.influx = None
        try:
            cls.moto = start_moto()
            create_images()
            cls.influx = start_influx()
        except Exception:
            if cls.moto is not None:
                cls.moto.kill()
            if cls.influx is not None:
                cls.influx.kill()
            raise

    @classmethod
    def tearDownClass(cls):
        if cls.influx is not None:
            cls.influx.kill()

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
            influx_options = InfluxOptions('localhost', 8086,
                                           'root', 'root', False)
            heka_options = None

            self.broker = Broker(
                self.io_loop, 'sqlite:////tmp/loads.db', '',
                heka_options,
                influx_options,
                aws_port=5000,
                aws_owner_id=None,
                aws_use_filters=False,
                aws_secret_key='xxx',
                aws_access_key='xxx')

            application.broker = self.broker
            return application
        except Exception:
            self.tearDownClass()
            raise

    def test_root(self):
        response = self.fetch('/api')
        self.assertTrue(response.code, 200)

        body = json.loads(response.body.decode())
        self.assertEqual(body['version'], __version__)
