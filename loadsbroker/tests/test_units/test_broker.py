from tornado.testing import AsyncTestCase

from moto import mock_ec2


class Test_broker(AsyncTestCase):
    def _createFUT(self):
        from loadsbroker.broker import Broker
        from loadsbroker.options import InfluxOptions, HekaOptions
        heka_options = HekaOptions("172.31.34.9", 6745, False)

        influx_options = InfluxOptions("localhost", 8086, "root", "root",
                                       False)

        return Broker(self.io_loop, "sqlite:////tmp/loads_test.db", None,
                      heka_options, influx_options,
                      aws_use_filters=False, initial_db=None)

    @mock_ec2
    def test_broker_creates(self):
        broker = self._createFUT()
        self.assertNotEqual(broker, None)
