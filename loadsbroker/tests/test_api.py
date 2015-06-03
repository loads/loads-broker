import json
from tornado.testing import AsyncHTTPTestCase
from loadsbroker.webapp import application
from loadsbroker.options import InfluxOptions, HekaOptions


class MockedDB:
    pass

class MockedBroker:
    db = MockedDB()

    def get_runs(self):
        return []


class HTTPApiTest(AsyncHTTPTestCase):
    db_uri = "sqlite:////tmp/loads_test.db"

    def get_app(self):
        application.broker = MockedBroker()
        return application

    def test_api(self):
        self.http_client.fetch(self.get_url('/api'), self.stop)
        response = self.wait()
        res = json.loads(response.body.decode())
        self.assertEqual(res['status'], 200)
        self.assertEqual(res['runs'], [])
