import unittest

from boto.ec2 import connect_to_region
from boto.ec2.image import Image
from moto import mock_ec2

from loadsbroker.awsctrl import _create_instance


class TestAWSCtrl(unittest.TestCase):

    @mock_ec2
    def test_create_instance(self):
        conn = connect_to_region('us-west-2')
        ami = 'ami-whatever'
        image = Image(conn)
        run_id = 'runid'
        num = 10
        instance_type = 't1.micro'
        user_data = None
        reserved_pool = []
        key_pair = 'loads'
        security = 'loads'
        sqluri = 'sqlite:////tmp/loads.db'
        res = []

        def created(*info):
            res.append(info)

        _create_instance(conn, run_id, num, ami, instance_type, user_data,
                         reserved_pool, key_pair, security, image,
                         sqluri, created)

        self.assertEqual(len(reserved_pool), 1)
        self.assertEqual(len(res), 1)
