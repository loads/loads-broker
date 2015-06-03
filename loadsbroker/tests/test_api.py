import json
import subprocess
import sys

from tornado.testing import AsyncHTTPTestCase
from loadsbroker.webapp import application
from loadsbroker.options import InfluxOptions, HekaOptions



def run_moto():
    args = [sys.executable, '-c',
            "from moto import server; server.main()",
            'ec2']
    return subprocess.Popen(args, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)


class HTTPApiTest(AsyncHTTPTestCase):
    db_uri = "sqlite:////tmp/loads_test.db"

    def setUp(self):
        self._p = run_moto()
        super().setUp()

    def tearDown(self):
        self._p.kill()
        super().tearDown()

    def get_app(self):
        application.broker = self._createBroker()
        return application

    def _createBroker(self):
        from loadsbroker.broker import Broker
        from loadsbroker.options import InfluxOptions, HekaOptions
        from mock import Mock
        return Broker(self.io_loop, self.db_uri, None,
                      Mock(spec=HekaOptions),
                      Mock(spec=InfluxOptions),
                      aws_use_filters=False, initial_db=None,
                      aws_port=500)

    def test_api(self):
        self.http_client.fetch(self.get_url('/api'), self.stop)
        response = self.wait()
        res = json.loads(response.body.decode())
        self.assertEqual(res['status'], 200)
        self.assertEqual(res['runs'], [])
