import json
import subprocess
import sys
import os

from tornado.testing import AsyncHTTPTestCase
from loadsbroker.webapp import application
from loadsbroker.options import InfluxOptions, HekaOptions
from loadsbroker import __version__


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
        self._broker = None
        super().setUp()

    def tearDown(self):
        if self._broker:
            self._broker.shutdown()
        self._p.kill()
        self._p.wait()
        super().tearDown()

    def get_app(self):
        self._broker = application.broker = self._createBroker()
        return application

    def _createBroker(self):
        from loadsbroker.broker import Broker
        from mock import Mock
        return Broker("1234", self.io_loop, self.db_uri, None,
                      Mock(spec=HekaOptions),
                      Mock(spec=InfluxOptions),
                      aws_use_filters=False, initial_db=None,
                      aws_port=5000)

    def test_api(self):
        self.http_client.fetch(self.get_url('/api'), self.stop)
        response = self.wait()
        res = json.loads(response.body.decode())
        self.assertEqual(res['status'], 200)
        self.assertEqual(res['runs'], [])

    def test_project(self):
        self.http_client.fetch(self.get_url('/api/project'), self.stop)
        response = self.wait()
        res = json.loads(response.body.decode())
        self.assertEqual(res['status'], 200)

        # adding a project
        request_json = os.path.join(os.path.dirname(__file__), 'request.json')

        with open(request_json) as f:
            data = json.loads(f.read())

        self.http_client.fetch(self.get_url('/api/project'), self.stop,
                               method="POST", body=json.dumps(data))
        response = self.wait()
        res = json.loads(response.body.decode())
        project_id = res['uuid']

        # we should have two plans
        self.assertEqual(len(res['plans']), 2)

        # the second one is "Moar Servers"
        plan_2 = res['plans'][1]
        self.assertEqual(plan_2['name'], 'Moar Servers')

        # with one step
        self.assertEqual(len(plan_2['steps']), 1)

        # checking the project exists
        self.http_client.fetch(self.get_url('/api/project'), self.stop)
        response = self.wait()
        res = json.loads(response.body.decode())
        self.assertTrue(project_id in [proj['uuid'] for proj in
                        res['projects']])

        # checking the project
        self.http_client.fetch(self.get_url('/api/project/%s' % project_id),
                               self.stop)
        response = self.wait()
        res = json.loads(response.body.decode())
        self.assertEqual(res['project']['name'], 'Push Testing')

        # deleting
        self.http_client.fetch(self.get_url('/api/project/%s' % project_id),
                               self.stop, method='DELETE')
        response = self.wait()

        self.http_client.fetch(self.get_url('/api/project'), self.stop)
        response = self.wait()
        res = json.loads(response.body.decode())
        self.assertTrue(project_id not in [proj['uuid'] for proj in
                        res['projects']])

    def test_limit_offset(self):
        self.http_client.fetch(self.get_url('/api?limit=1&offset=0'),
                               self.stop)
        response = self.wait()
        res = json.loads(response.body.decode())
        self.assertEqual(res['status'], 200)
        self.assertEqual(res['runs'], [])

    def test_swagger(self):
        self.http_client.fetch(self.get_url('/__api__'), self.stop)
        response = self.wait()
        res = json.loads(response.body.decode())
        self.assertEqual(res['info']['version'], __version__)
