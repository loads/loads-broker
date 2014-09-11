import subprocess
import sys
import unittest
import requests
import time

from loadsbroker.client import Client
from loadsbroker import __version__


class TestClient(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        cmd = 'from loadsbroker.main import main; main()'
        cmd = '%s -c "%s"' % (sys.executable, cmd)
        broker = subprocess.Popen(cmd, shell=True,
                                  stdout=subprocess.PIPE,
                                  stderr=subprocess.PIPE)

        # wait for the broker to be ready
        starting = time.time()
        started = False

        errors = []

        while time.time() - starting < 1:
            try:
                requests.get('http://127.0.0.1:8080', timeout=.1)
                started = True
                break
            except Exception as exc:
                errors.append(exc)
                time.sleep(.1)

        if not started:
            print('Could not start the broker!')
            broker.kill()
            if len(errors) > 0:
                raise errors[-1]
            else:
                raise Exception()

        cls.broker = broker
        cls.client = Client()

    @classmethod
    def tearDownClass(cls):
        try:
            cls.broker.terminate()
        finally:
            cls.broker.kill()

    def test_info(self):
        res = self.client('info')
        self.assertEqual(res['version'], __version__)

    def test_launch_run(self):

        res = self.client('run', nodes=3)
        self.assertTrue('run_id' in res)
        run_id = res['run_id']
        self.assertEquals(3, res['nodes'])

        # checking the run exists
        res = self.client('status', run_id)

        # aborting the run
        res = self.client('abort', run_id)

        # checking the run is not listed anymore
        res = self.client('status', run_id)
