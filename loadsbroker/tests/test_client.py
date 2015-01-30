import sys
import unittest
import json
from io import StringIO
import shlex
import time
from signal import SIGKILL, SIGTERM

from loadsbroker.client import main
from loadsbroker.tests.util import start_all
from loadsbroker.db import Database, Plan, Step


class TestClient(unittest.TestCase):

    def setUp(self):
        super(TestClient, self).setUp()
        self.db = Database('sqlite:////tmp/loads.db')
        self.session = self.db.session()
        self.sqlobs = []

    def tearDown(self):
        for ob in self.sqlobs:
            self.session.delete(ob)
        self.session.commit()
        super(TestClient, self).tearDown()

    @classmethod
    def setUpClass(cls):
        cls.broker, cls.moto, cls.docker, cls.influx = start_all()

    @classmethod
    def tearDownClass(cls):
        try:
            cls.broker.terminate()
            time.sleep(.5)
        finally:
            cls.broker.kill()

        cls.moto.kill()
        cls.docker.kill()
        cls.influx.kill()
        cls.broker.wait()

        if cls.broker.returncode not in (0, -SIGKILL, -SIGTERM):
            errors = cls.broker.stderr.read()
            if len(errors) > 0:
                raise Exception(errors.decode())

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

    def _create_strategy(self, name='nawak'):
        url = "https://s3.amazonaws.com/loads-images/simpletest-dev.tar.gz"

        cs = Step(
            name='yeah',
            instance_count=1,
            container_name="bbangert/simpletest:dev",
            container_url=url)

        plan = Plan(name=name, uuid=name,
                    steps=[cs])

        self.session.add(plan)
        self.session.commit()
        self.sqlobs.append(plan)
        self.sqlobs.append(cs)

        return plan

    def _launch_run(self):
        # running a launch with a bad strategy id should lead to a
        # 404
        res = self._main('run --nodes 3 --strategy-id nawak')
        self.assertFalse(res['success'])
        self.assertEqual(res['status'], 404)

        # let's create a strategy now
        self._create_strategy()

        # this should work
        res = self._main('run --nodes 3 --strategy-id nawak')
        self.assertTrue('run_id' in res, res)

        run_id = res['run_id']

        # checking a random uid leads to a 404
        res = self._main('status meh')
        self.assertFalse(res['success'])
        self.assertEqual(res['status'], 404)

        # checking the run exists
        res = self._main('status %s' % run_id)
        self.assertTrue('run' in res, res)

        res = res['run']
        wanted = {'uuid': run_id, 'state': 0}

        for key, val in wanted.items():
            self.assertEqual(res[key], val)

        # checking the run is listed in the info
        res = self._main('info')
        uuids = [r['uuid'] for r in res['runs']]
        self.assertTrue(run_id in uuids)

        # checking aborting a random uid leads to a 404
        res = self._main('abort meh')
        self.assertFalse(res['success'])
        self.assertEqual(res['status'], 404)

        # aborting the run
        res = self._main('abort %s' % run_id)
        self.assertTrue(res['success'])

        # aborting the run again should lead to an error
        res = self._main('abort %s' % run_id)
        self.assertFalse(res['success'], res)

        # checking the run is not running anymore
        res = self._main('status %s' % run_id)

        # we can also delete a run for ever
        res = self._main('delete %s' % run_id)
        self.assertTrue(res['success'], res)

        # a second call fails
        res = self._main('delete %s' % run_id)
        self.assertFalse(res['success'])

        # and the run dissapears from the list of runs
        res = self._main('info')
        uuids = [r['uuid'] for r in res['runs']]
        self.assertFalse(run_id in uuids)
