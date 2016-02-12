import os

import boto
from tornado import gen
from mock import Mock, PropertyMock, patch
from moto import mock_ec2
from tornado.testing import AsyncTestCase, gen_test


here_dir = os.path.dirname(os.path.abspath(__file__))
ec2_mocker = mock_ec2()
_BOTO = os.path.join(os.path.expanduser('~'), '.boto')


def setUp():
    if os.path.exists(_BOTO):
        boto.config.clear()
    ec2_mocker.start()


def tearDown():
    ec2_mocker.stop()
    if os.path.exists(_BOTO):
        boto.config.load_from_path(_BOTO)


class Test_broker(AsyncTestCase):
    db_uri = "sqlite:////tmp/loads_test.db"

    def _createFUT(self):
        from loadsbroker.broker import Broker
        from loadsbroker.options import InfluxOptions, HekaOptions

        return Broker(self.io_loop, self.db_uri, None,
                      Mock(spec=HekaOptions),
                      Mock(spec=InfluxOptions),
                      aws_use_filters=False, initial_db=None)

    def test_broker_creation(self):
        broker = self._createFUT()
        self.assertNotEqual(broker, None)
        broker.shutdown()

    def test_broker_run_plan(self):
        from tornado.concurrent import Future
        # Setup all the mocks
        mock_future = Mock(spec=Future)

        # Setup the mock RunManager instance, and properties needed
        mock_rm_inst = Mock()

        mock_run = Mock()
        type(mock_run).uuid = PropertyMock(return_value="asdf")

        type(mock_rm_inst).run = PropertyMock(return_value=mock_run)

        with patch('loadsbroker.broker.RunManager',
                   new_callable=Mock) as mock_rm:
            broker = self._createFUT()
            mock_rm.new_run.return_value = (mock_rm_inst, mock_future)
            uuid = broker.run_plan("bleh", create_db=False, creator='tarek')
            self.assertEqual(uuid, "asdf")


file_name = "/tmp/loads_test.db"
db_uri = "sqlite:///" + file_name


class Test_run_manager(AsyncTestCase):

    def setUp(self):
        super().setUp()
        from loadsbroker.db import Database
        from loadsbroker.db import setup_database

        self.db = Database(db_uri, echo=True)
        self.db_session = self.db.session()
        setup_database(self.db_session, os.path.join(here_dir, "testdb.json"))

    def tearDown(self):
        super().tearDown()
        import loadsbroker.aws
        loadsbroker.aws.AWS_AMI_IDS = {k: {} for k in
                                       loadsbroker.aws.AWS_REGIONS}
        self.helpers = None
        self.db = None
        self.db_session = None
        try:
            os.remove(file_name)
        except FileNotFoundError:
            pass

    @gen.coroutine
    def _createFUT(self, plan_uuid=None, run_uuid=None, **kwargs):
        from loadsbroker.broker import RunManager, RunHelpers
        from loadsbroker.extensions import Docker, DNSMasq, Ping, Heka, SSH
        from loadsbroker.aws import EC2Pool
        from loadsbroker.db import Plan, Run

        if not plan_uuid:
            plan_uuid = self.db_session.query(Plan).limit(1).one().uuid

        region = "us-west-2"
        # Setup the AMI we need available to make instances
        conn = boto.ec2.connect_to_region(region)
        reservation = conn.run_instances('ami-1234abcd',
                                         instance_type='m1.small')
        instance = reservation.instances[0]
        conn.create_image(instance.id, "CoreOS stable")

        kwargs = {}
        kwargs["io_loop"] = self.io_loop
        kwargs["use_filters"] = False
        pool = EC2Pool("broker_1234", **kwargs)
        yield pool.ready

        helpers = RunHelpers()
        helpers.ping = Mock(spec=Ping)
        helpers.docker = Mock(spec=Docker)
        helpers.dns = Mock(spec=DNSMasq)
        helpers.heka = Mock(spec=Heka)
        helpers.ssh = Mock(spec=SSH)

        @gen.coroutine
        def return_none(*args, **kwargs):
            return None
        helpers.docker.setup_collection = return_none
        helpers.docker.wait = return_none
        helpers.docker.load_containers = return_none
        self.helpers = helpers

        run = Run.new_run(self.db_session, plan_uuid)
        self.db_session.add(run)
        self.db_session.commit()

        rmg = RunManager(helpers, self.db_session,  pool, self.io_loop, run)
        rmg.run_env.update(**kwargs)
        return rmg

    @gen_test(timeout=10)
    def test_create(self):
        rm = yield self._createFUT()
        assert rm is not None

    @gen_test(timeout=10)
    def test_initialize(self):
        from loadsbroker.db import RUNNING, INITIALIZING
        rm = yield self._createFUT()

        self.assertEqual(rm.state, INITIALIZING)
        yield rm._initialize()
        self.assertEqual(rm.state, RUNNING)

    @gen_test(timeout=10)
    def test_run(self):
        from loadsbroker.db import (
            RUNNING, INITIALIZING, TERMINATING, COMPLETED
        )
        rm = yield self._createFUT()

        self.assertEqual(rm.state, INITIALIZING)
        yield rm._initialize()
        self.assertEqual(rm.state, RUNNING)
        rm.sleep_time = 0.5

        run_j = rm.run.json()
        self.assertEqual(run_j['plan_id'], 1)
        self.assertEqual(run_j['plan_name'], 'Single Server')

        # Zero out extra calls
        @gen.coroutine
        def zero_out(*args, **kwargs):
            return None
        self.helpers.ssh.reload_sysctl = zero_out
        self.helpers.heka.start = zero_out
        self.helpers.dns.start = zero_out
        self.helpers.docker.run_containers = zero_out
        self.helpers.docker.stop_containers = zero_out
        self.helpers.heka.stop = zero_out
        self.helpers.dns.stop = zero_out

        # Ensure instances all report as done after everything
        # has been started
        @gen.coroutine
        def return_true(*args, **kwargs):
            return not all([s.ec2_collection.started for s in rm._set_links])
        self.helpers.docker.is_running = return_true

        result = yield rm._run()
        self.assertEqual(rm.state, TERMINATING)

        result = yield rm._shutdown()
        self.assertEqual(rm.state, COMPLETED)
        self.assertEqual(result, None)

    @gen_test(timeout=20)
    def test_abort(self):
        from loadsbroker.db import (
            RUNNING, INITIALIZING, TERMINATING
        )
        rm = yield self._createFUT()
        self.assertEqual(rm.state, INITIALIZING)
        yield rm._initialize()
        self.assertEqual(rm.state, RUNNING)
        rm.sleep_time = 0.5

        # Zero out extra calls
        @gen.coroutine
        def zero_out(*args, **kwargs):
            return None
        self.helpers.ssh.reload_sysctl = zero_out
        self.helpers.heka.start = zero_out
        self.helpers.dns.start = zero_out
        self.helpers.docker.run_containers = zero_out
        self.helpers.docker.stop_containers = zero_out
        self.helpers.heka.stop = zero_out
        self.helpers.dns.stop = zero_out

        # Ensure instances all report as done after everything
        # has been started
        @gen.coroutine
        def return_true(*args, **kwargs):
            all_started = all([s.ec2_collection.started
                               for s in rm._set_links])
            if all_started:
                rm.abort = True
            return True
        self.helpers.docker.is_running = return_true

        result = yield rm._run()
        self.assertEqual(rm.state, TERMINATING)
        self.assertEqual(result, None)
        self.assertEqual([s.ec2_collection.finished for s in rm._set_links],
                         [False, False])
