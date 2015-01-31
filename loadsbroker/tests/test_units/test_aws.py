import unittest
from tornado.testing import AsyncTestCase, gen_test
import boto
from moto import mock_ec2

from freezegun import freeze_time


class Test_populate_ami_ids(unittest.TestCase):
    def tearDown(self):
        import loadsbroker.aws
        loadsbroker.aws.AWS_AMI_IDS = {k: {} for k in
                                       loadsbroker.aws.AWS_REGIONS}

    @mock_ec2
    def test_no_instances(self):
        import loadsbroker.aws
        self.assertEqual(len(loadsbroker.aws.AWS_AMI_IDS),
                         len(loadsbroker.aws.AWS_REGIONS))
        first_region = loadsbroker.aws.AWS_REGIONS[0]
        self.assertEqual(len(loadsbroker.aws.AWS_AMI_IDS[first_region]), 0)
        loadsbroker.aws.populate_ami_ids(use_filters=False)
        self.assertEqual(len(loadsbroker.aws.AWS_AMI_IDS[first_region]), 0)

    @mock_ec2
    def test_ami_is_found(self):
        import loadsbroker.aws
        first_region = loadsbroker.aws.AWS_REGIONS[0]
        self.assertEqual(len(loadsbroker.aws.AWS_AMI_IDS[first_region]), 0)

        # Populate a few instances into the mock ec2
        conn = boto.ec2.connect_to_region(first_region)
        reservation = conn.run_instances('ami-1234abcd')
        instance = reservation.instances[0]
        conn.create_image(instance.id, "CoreOS stable")

        loadsbroker.aws.populate_ami_ids(use_filters=False)
        self.assertEqual(len(loadsbroker.aws.AWS_AMI_IDS[first_region]), 1)


class Test_get_ami(unittest.TestCase):
    def tearDown(self):
        import loadsbroker.aws
        loadsbroker.aws.AWS_AMI_IDS = {k: {} for k in
                                       loadsbroker.aws.AWS_REGIONS}

    @mock_ec2
    def test_image_found(self):
        import loadsbroker.aws
        first_region = loadsbroker.aws.AWS_REGIONS[0]

        # Stash an image for a m1.small
        conn = boto.ec2.connect_to_region(first_region)
        reservation = conn.run_instances("ami-1234abcd",
                                         instance_type='m1.small')
        instance = reservation.instances[0]
        image_id = conn.create_image(instance.id, "CoreOS stable")
        img = conn.get_all_images()[0]

        loadsbroker.aws.AWS_AMI_IDS[first_region] = {"paravirtual": img}

        ami_id = loadsbroker.aws.get_ami(first_region, "m1.small")
        self.assertEqual(ami_id, image_id)

    def test_no_image_found(self):
        import loadsbroker.aws
        first_region = loadsbroker.aws.AWS_REGIONS[0]
        self.assertRaises(KeyError, loadsbroker.aws.get_ami, first_region,
                          "m1.small")


class Test_available_instance(unittest.TestCase):
    def _callFUT(self, instance):
        from loadsbroker.aws import available_instance
        return available_instance(instance)

    @mock_ec2
    def test_running_instance_usable(self):
        # Setup a running instance
        conn = boto.connect_ec2()
        conn.run_instances("ami-1234abcd")
        reservations = conn.get_all_instances()
        instance = reservations[0].instances[0]

        self.assertTrue(self._callFUT(instance))

    @mock_ec2
    def test_pending_instance_usable(self):
        with freeze_time("2012-01-14 03:21:34"):
            # Setup a running instance
            conn = boto.connect_ec2('the_key', 'the_secret')
            reservation = conn.run_instances("ami-1234abcd")
            instance = reservation.instances[0]

        with freeze_time("2012-01-14 03:22:34"):
            self.assertTrue(self._callFUT(instance))

    @mock_ec2
    def test_pending_instance_unusable(self):
        # Setup a running instance
        with freeze_time("2012-01-14 03:21:34"):
            conn = boto.connect_ec2()
            reservation = conn.run_instances("ami-1234abcd")
            instance = reservation.instances[0]

        with freeze_time("2012-01-14 03:24:34"):
            self.assertFalse(self._callFUT(instance))


class Test_ec2_collection(AsyncTestCase):
    def _callFUT(self, run_id, uuid, conn, instances):
        from loadsbroker.aws import EC2Collection
        return EC2Collection(run_id, uuid, conn, instances, self.io_loop)

    @mock_ec2
    def test_collection_creation(self):
        # Get some instances
        conn = boto.connect_ec2()
        reservation = conn.run_instances("ami-1234abcd", 5)
        coll = self._callFUT("a", "b", conn, reservation.instances)
        self.assertEqual(len(coll.instances), len(reservation.instances))

    @mock_ec2
    def test_instance_status_checks(self):
        conn = boto.connect_ec2()
        reservation = conn.run_instances("ami-1234abcd", 5)
        coll = self._callFUT("a", "b", conn, reservation.instances)
        self.assertEqual(len(coll.instances), len(coll.pending_instances()))

        # Now with running instances
        for inst in coll.instances:
            inst.instance.start()
            inst.instance.update()
        self.assertEqual(len(coll.instances), len(coll.running_instances()))

        # Now the stopped instances
        for inst in coll.instances:
            inst.instance.stop()
            inst.instance.update()
        self.assertEqual(len(coll.instances), len(coll.dead_instances()))

    @mock_ec2
    @gen_test
    def test_remove_unresponsive_instances(self):
        conn = boto.connect_ec2()
        reservation = conn.run_instances("ami-1234abcd", 5)
        coll = self._callFUT("a", "b", conn, reservation.instances)

        # Mark all the instances as non-responsive
        for inst in coll.instances:
            inst.state.nonresponsive = True

        # Remove the 'dead' instances
        yield coll.remove_dead_instances()
        self.assertEqual(len(coll.instances), 0)

    @mock_ec2
    @gen_test
    def test_instance_waiting(self):
        conn = boto.connect_ec2()
        reservation = conn.run_instances("ami-1234abcd", 5)
        coll = self._callFUT("a", "b", conn, reservation.instances)

        for inst in coll.instances:
            self.assertEqual(inst.instance.state, "pending")
        for inst in coll.instances:
            inst.instance.start()
            inst.instance.update()
        yield coll.wait_for_running()
        for inst in coll.instances:
            self.assertEqual(inst.instance.state, "running")


class Test_ec2_pool(AsyncTestCase):
    def _callFUT(self, broker_id, **kwargs):
        from loadsbroker.aws import EC2Pool
        kwargs["io_loop"] = self.io_loop
        kwargs["use_filters"] = False
        return EC2Pool(broker_id, **kwargs)

    @mock_ec2
    @gen_test
    def test_empty_pool(self):
        pool = self._callFUT("br12")
        # Wait for initialization to finish
        yield pool.ready
        self.assertEqual(pool._recovered, {})
        for _, val in pool._instances.items():
            self.assertEqual(val, [])

    @mock_ec2
    @gen_test
    def test_recovered_instances(self):
        import loadsbroker.aws
        # First, add some instances
        first_region = loadsbroker.aws.AWS_REGIONS[0]
        conn = boto.ec2.connect_to_region(first_region)
        reservation = conn.run_instances("ami-1234abcd", 5,
                                         instance_type='m1.small')

        # Start them up and assign some tags
        for inst in reservation.instances:
            inst.start()
        conn.create_tags([x.id for x in reservation.instances],
                         {"Name": "loads-br12",
                          "Project": "loads",
                          })

        # Get the pool
        pool = self._callFUT("br12")
        yield pool.ready

        # Verify 5 instances recovered
        self.assertEqual(len(pool._instances[first_region]), 5)

    @mock_ec2
    @gen_test
    def test_allocates_instances_for_collection(self):
        region = "us-west-2"
        # Setup the AMI we need available to make instances
        conn = boto.ec2.connect_to_region(region)
        reservation = conn.run_instances('ami-1234abcd')
        instance = reservation.instances[0]
        conn.create_image(instance.id, "CoreOS stable")

        # Now run the test
        pool = self._callFUT("br12")
        yield pool.ready

        coll = yield pool.request_instances("run_12", "12423", 5,
                                            inst_type="m1.small",
                                            region=region)
        self.assertEqual(len(coll.instances), 5)

    @mock_ec2
    @gen_test
    def test_allocates_recovered_for_collection(self):
        region = "us-west-2"

        # First, add some instances
        conn = boto.ec2.connect_to_region(region)
        reservation = conn.run_instances("ami-1234abcd", 5,
                                         instance_type='m1.small')
        instance = reservation.instances[0]
        # Make the AMI we need available
        conn.create_image(instance.id, "CoreOS stable")

        # Start them up and assign some tags
        for inst in reservation.instances:
            inst.start()
        conn.create_tags([x.id for x in reservation.instances],
                         {"Name": "loads-br12",
                          "Project": "loads",
                          })

        # Now run the test
        pool = self._callFUT("br12")
        yield pool.ready

        self.assertEqual(len(pool._instances[region]), 5)

        coll = yield pool.request_instances("run_12", "12423", 5,
                                            inst_type="m1.small",
                                            region=region)
        self.assertEqual(len(coll.instances), 5)
        self.assertEqual(len(pool._instances[region]), 0)

    @mock_ec2
    @gen_test
    def test_allocate_ignores_already_assigned(self):
        region = "us-west-2"

        # First, add some instances
        conn = boto.ec2.connect_to_region(region)
        reservation = conn.run_instances("ami-1234abcd", 5,
                                         instance_type='m1.small')
        instance = reservation.instances[0]
        # Make the AMI we need available
        conn.create_image(instance.id, "CoreOS stable")

        # Start them up and assign some tags
        for inst in reservation.instances:
            inst.start()
        conn.create_tags([x.id for x in reservation.instances],
                         {"Name": "loads-br12",
                          "Project": "loads",
                          "RunId": "asdf",
                          "Uuid": "hjkl",
                          })

        # Now run the test
        pool = self._callFUT("br12")
        yield pool.ready

        self.assertEqual(len(pool._instances[region]), 0)
        self.assertEqual(len(pool._recovered[("asdf", "hjkl")]), 5)

        coll = yield pool.request_instances("run_12", "12423", 5,
                                            inst_type="m1.small",
                                            region=region)
        self.assertEqual(len(coll.instances), 5)

    @mock_ec2
    @gen_test
    def test_allocate_returns_running_instances_only(self):
        region = "us-west-2"

        # First, add some instances
        conn = boto.ec2.connect_to_region(region)
        reservation = conn.run_instances("ami-1234abcd", 3,
                                         instance_type='m1.small')
        instance = reservation.instances[0]
        # Make the AMI we need available
        conn.create_image(instance.id, "CoreOS stable")

        # Start them up and assign some tags
        for inst in reservation.instances:
            inst.start()
        conn.create_tags([x.id for x in reservation.instances],
                         {"Name": "loads-br12",
                          "Project": "loads",
                          "RunId": "asdf",
                          "Uuid": "hjkl",
                          })

        # Now run the test
        pool = self._callFUT("br12")
        yield pool.ready
        pool.use_filters = True

        self.assertEqual(len(pool._recovered[("asdf", "hjkl")]), 3)

        coll = yield pool.request_instances("asdf", "hjkl", 5,
                                            inst_type="m1.small",
                                            region=region,
                                            allocate_missing=False)
        self.assertEqual(len(coll.instances), 3)
        self.assertEqual(len(pool._recovered[("asdf", "hjkl")]), 0)

    @mock_ec2
    @gen_test
    def test_returning_instances(self):
        region = "us-west-2"
        # Setup the AMI we need available to make instances
        conn = boto.ec2.connect_to_region(region)
        reservation = conn.run_instances('ami-1234abcd')
        instance = reservation.instances[0]
        conn.create_image(instance.id, "CoreOS stable")

        # Now run the test
        pool = self._callFUT("br12")
        yield pool.ready
        pool.use_filters = True

        coll = yield pool.request_instances("run_12", "12423", 5,
                                            inst_type="m1.small",
                                            region=region)
        yield coll.wait_for_running()
        self.assertEqual(len(coll.instances), 5)

        # Return them
        yield pool.release_instances(coll)
        self.assertEqual(len(pool._instances[region]), 5)

        # Acquire 5 again
        coll = yield pool.request_instances("run_12", "42315", 5,
                                            inst_type="m1.small",
                                            region=region)
        self.assertEqual(len(coll.instances), 5)
        self.assertEqual(len(pool._instances[region]), 0)

    @mock_ec2
    @gen_test
    def test_reaping_all_instances(self):
        region = "us-west-2"
        # Setup the AMI we need available to make instances
        conn = boto.ec2.connect_to_region(region)
        reservation = conn.run_instances('ami-1234abcd')
        instance = reservation.instances[0]
        conn.create_image(instance.id, "CoreOS stable")

        # Now run the test
        pool = self._callFUT("br12")
        yield pool.ready
        pool.use_filters = True

        coll = yield pool.request_instances("run_12", "12423", 5,
                                            inst_type="m1.small",
                                            region=region)
        yield coll.wait_for_running()
        self.assertEqual(len(coll.instances), 5)

        # Return them
        yield pool.release_instances(coll)
        self.assertEqual(len(pool._instances[region]), 5)

        # Now, reap them
        yield pool.reap_instances()
        self.assertEqual(len(pool._instances[region]), 0)
