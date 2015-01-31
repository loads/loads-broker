import os

from mock import Mock, PropertyMock, patch
from moto import mock_ec2
from tornado.testing import AsyncTestCase


class Test_broker(AsyncTestCase):
    db_uri = "sqlite:////tmp/loads_test.db"

    def setUp(self):
        super().setUp()
        from loadsbroker.db import Database
        self.db = Database(self.db_uri, echo=True)
        self.db_session = self.db.session()

    def tearDown(self):
        super().tearDown()
        try:
            os.unlink(self.db_uri)
        except FileNotFoundError:
            pass

    def _createFUT(self):
        from loadsbroker.broker import Broker
        from loadsbroker.options import InfluxOptions, HekaOptions
        heka_options = HekaOptions("172.31.34.9", 6745, False)
        influx_options = InfluxOptions("localhost", 8086, "root", "root",
                                       False)
        return Broker(self.io_loop, self.db_uri, None,
                      heka_options, influx_options,
                      aws_use_filters=False, initial_db=None)

    @mock_ec2
    def test_broker_creation(self):
        broker = self._createFUT()
        self.assertNotEqual(broker, None)
        broker.shutdown()

    @mock_ec2
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
            uuid = broker.run_plan("bleh", create_db=False)
            self.assertEqual(uuid, "asdf")
