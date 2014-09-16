import subprocess
import sys
import unittest
import requests
import time
import json
from io import StringIO
import shlex

from loadsbroker.client import main
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

    @classmethod
    def tearDownClass(cls):
        try:
            cls.broker.terminate()
        finally:
            cls.broker.kill()

    def _main(self, cmd):
        cmd = shlex.split(cmd)
        old = sys.stdout
        sys.stdout = StringIO()
        try:
            main(cmd)
        finally:
            sys.stdout.seek(0)
            res = sys.stdout.read().strip()
            sys.stdout = old
        return json.loads(res)

    def test_info(self):
        res = self._main('info')
        self.assertEqual(res['version'], __version__)

    def test_launch_run(self):

        res = self._main('run --nodes 3')
        self.assertTrue('run_id' in res)
        run_id = res['run_id']
        self.assertEquals(3, res['nodes'])

        # checking a random uid leads to a 404
        res = self._main('status meh')
        self.assertFalse(res['success'])
        self.assertEqual(res['status'], 404)

        # checking the run exists
        res = self._main('status %s' % run_id)['run']
        wanted = {'ami': 'ami-3193e801', 'uuid': run_id,
                  'state': 0}
        for key, val in wanted.items():
            self.assertEqual(res[key], val)

        # checking aborting a random uid leads to a 404
        res = self._main('abort meh')
        self.assertFalse(res['success'])
        self.assertEqual(res['status'], 404)

        # aborting the run
        res = self._main('abort %s' % run_id)
        self.assertTrue(res['success'])

        # aborting the run again should lead to an error
        res = self._main('abort %s' % run_id)
        self.assertFalse(res['success'])

        # checking the run is not running anymore
        res = self._main('status %s' % run_id)
