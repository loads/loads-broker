import json
import subprocess
import sys
import os

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

    def test_project(self):
        self.http_client.fetch(self.get_url('/api/project'), self.stop)
        response = self.wait()
        res = json.loads(response.body.decode())
        self.assertEqual(res['status'], 200)

        # adding a project
        data = {'name': 'My project'}
        self.http_client.fetch(self.get_url('/api/project'), self.stop,
                               method="POST", body=json.dumps(data))
        response = self.wait()
        res = json.loads(response.body.decode())
        project_id = res['id']

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
        self.assertEqual(res['project']['name'], 'My project')

        # deleting
        self.http_client.fetch(self.get_url('/api/project/%s' % project_id),
                               self.stop, method='DELETE')
        response = self.wait()

        self.http_client.fetch(self.get_url('/api/project'), self.stop)
        response = self.wait()
        res = json.loads(response.body.decode())
        self.assertTrue(project_id not in [proj['uuid'] for proj in
                        res['projects']])
