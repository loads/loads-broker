import unittest
from tornado.testing import AsyncTestCase, gen_test
from moto import mock_ec2
import boto

from freezegun import freeze_time
from loadsbroker.tests.util import (clear_boto_context, load_boto_context,
                                    create_image)


ec2_mocker = mock_ec2()
_OLD_CONTEXT = []


def setUp():
    _OLD_CONTEXT[:] = list(clear_boto_context())
    ec2_mocker.start()
    create_image()


def tearDown():
    ec2_mocker.stop()
    load_boto_context(*_OLD_CONTEXT)


def nuke_backend():
    for backend in ec2_mocker.backends.values():
        backend.reset()


class Test_populate_ami_ids(unittest.TestCase):
    def setUp(self):
        # Nuke the backend
        nuke_backend()

    def tearDown(self):
        import loadsbroker.aws
        loadsbroker.aws.AWS_AMI_IDS = {k: {} for k in
                                       loadsbroker.aws.AWS_REGIONS}

    def test_no_instances(self):
        import loadsbroker.aws
        self.assertEqual(len(loadsbroker.aws.AWS_AMI_IDS),
                         len(loadsbroker.aws.AWS_REGIONS))
        first_region = loadsbroker.aws.AWS_REGIONS[0]
        self.assertEqual(len(loadsbroker.aws.AWS_AMI_IDS[first_region]), 0)
        loadsbroker.aws.populate_ami_ids(use_filters=False)
        self.assertEqual(len(loadsbroker.aws.AWS_AMI_IDS[first_region]), 0)

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
    def setUp(self):
        # Nuke the backend
        nuke_backend()

    def tearDown(self):
        super().tearDown()
        import loadsbroker.aws
        loadsbroker.aws.AWS_AMI_IDS = {k: {} for k in
                                       loadsbroker.aws.AWS_REGIONS}

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
    def setUp(self):
        # Nuke the backend
        nuke_backend()

    def _callFUT(self, instance):
        from loadsbroker.aws import available_instance
        return available_instance(instance)

    def test_running_instance_usable(self):
        # Setup a running instance
        conn = boto.connect_ec2()
        conn.run_instances("ami-1234abcd")
        reservations = conn.get_all_instances()
        instance = reservations[0].instances[0]

        self.assertTrue(self._callFUT(instance))

    def test_pending_instance_usable(self):
        with freeze_time("2012-01-14 03:21:34"):
            # Setup a running instance
            conn = boto.connect_ec2('the_key', 'the_secret')
            reservation = conn.run_instances("ami-1234abcd")
            instance = reservation.instances[0]

        with freeze_time("2012-01-14 03:22:34"):
            self.assertTrue(self._callFUT(instance))

    def test_pending_instance_unusable(self):
        # Setup a running instance
        with freeze_time("2012-01-14 03:21:34"):
            conn = boto.connect_ec2()
            reservation = conn.run_instances("ami-1234abcd")
            instance = reservation.instances[0]

        with freeze_time("2012-01-14 03:24:34"):
            self.assertFalse(self._callFUT(instance))


class Test_ec2_collection(AsyncTestCase):
    def setUp(self):
        super().setUp()
        # Nuke the backend
        nuke_backend()

    def _callFUT(self, run_id, uuid, conn, instances):
        from loadsbroker.aws import EC2Collection
        return EC2Collection(run_id, uuid, conn, instances, self.io_loop)

    def test_collection_creation(self):
        # Get some instances
        conn = boto.connect_ec2()
        reservation = conn.run_instances("ami-1234abcd", 5)
        coll = self._callFUT("a", "b", conn, reservation.instances)
        self.assertEqual(len(coll.instances), len(reservation.instances))

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

    @gen_test
    async def test_remove_unresponsive_instances(self):
        conn = boto.connect_ec2()
        reservation = conn.run_instances("ami-1234abcd", 5)
        coll = self._callFUT("a", "b", conn, reservation.instances)

        # Mark all the instances as non-responsive
        for inst in coll.instances:
            inst.state.nonresponsive = True

        # Remove the 'dead' instances
        await coll.remove_dead_instances()
        self.assertEqual(len(coll.instances), 0)

    @gen_test
    async def test_instance_waiting(self):
        conn = boto.connect_ec2()
        reservation = conn.run_instances("ami-1234abcd", 5)
        coll = self._callFUT("a", "b", conn, reservation.instances)

        for inst in coll.instances:
            self.assertEqual(inst.instance.state, "pending")
        for inst in coll.instances:
            inst.instance.start()
            inst.instance.update()
        await coll.wait_for_running()
        for inst in coll.instances:
            self.assertEqual(inst.instance.state, "running")


class Test_ec2_pool(AsyncTestCase):
    def setUp(self):
        super().setUp()
        # Nuke the backend
        nuke_backend()

    def _callFUT(self, broker_id, **kwargs):
        from loadsbroker.aws import EC2Pool
        kwargs["io_loop"] = self.io_loop
        kwargs["use_filters"] = False
        return EC2Pool(broker_id, **kwargs)

    @gen_test
    async def test_empty_pool(self):
        pool = self._callFUT("br12")
        # Wait for initialization to finish
        await pool.ready
        self.assertEqual(pool._recovered, {})
        for _, val in pool._instances.items():
            self.assertEqual(val, [])

    @gen_test(timeout=10)
    async def test_recovered_instances(self):
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
        await pool.ready

        # Verify 5 instances recovered
        self.assertEqual(len(pool._instances[first_region]), 5)

    @gen_test
    async def test_allocates_instances_for_collection(self):
        region = "us-west-2"
        # Setup the AMI we need available to make instances
        conn = boto.ec2.connect_to_region(region)
        reservation = conn.run_instances('ami-1234abcd')
        instance = reservation.instances[0]
        conn.create_image(instance.id, "CoreOS stable")

        # Now run the test
        pool = self._callFUT("br12")
        await pool.ready

        coll = await pool.request_instances("run_12", "12423", 5,
                                            inst_type="m1.small",
                                            region=region)
        self.assertEqual(len(coll.instances), 5)

    @gen_test
    async def test_owner_tags(self):
        region = "us-west-2"
        conn = boto.ec2.connect_to_region(region)
        reservation = conn.run_instances('ami-1234abcd')
        instance = reservation.instances[0]
        conn.create_image(instance.id, "CoreOS stable")

        broker_id = "br12"
        owner = "otto.push"

        pool = self._callFUT(broker_id)
        pool.use_filters = True
        await pool.ready

        coll = await pool.request_instances("run_12", "12423", 5,
                                            inst_type="m1.small",
                                            region=region,
                                            owner=owner)
        ids = {ec2instance.instance.id for ec2instance in coll.instances}

        tagged = 0
        for reservation in conn.get_all_instances():
            for instance in reservation.instances:
                if instance.id not in ids:
                    continue
                self.assertEqual(instance.tags['Owner'], owner)
                self.assertEqual(instance.tags['Name'],
                                 "loads-{}-{}".format(broker_id, owner))
                tagged += 1
        self.assertEqual(tagged, len(ids))

    @gen_test
    async def test_allocates_recovered_for_collection(self):
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
        await pool.ready

        self.assertEqual(len(pool._instances[region]), 5)

        coll = await pool.request_instances("run_12", "12423", 5,
                                            inst_type="m1.small",
                                            region=region)
        self.assertEqual(len(coll.instances), 5)
        self.assertEqual(len(pool._instances[region]), 0)

    @gen_test
    async def test_allocate_ignores_already_assigned(self):
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
        await pool.ready

        self.assertEqual(len(pool._instances[region]), 0)
        self.assertEqual(len(pool._recovered[("asdf", "hjkl")]), 5)
        coll = await pool.request_instances("run_12", "12423", 5,
                                            inst_type="m1.small",
                                            region=region)
        self.assertEqual(len(coll.instances), 5)

    @gen_test
    async def test_allocate_returns_running_instances_only(self):
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
        await pool.ready
        pool.use_filters = True

        self.assertEqual(len(pool._recovered[("asdf", "hjkl")]), 3)

        coll = await pool.request_instances("asdf", "hjkl", 5,
                                            inst_type="m1.small",
                                            region=region,
                                            allocate_missing=False)
        self.assertEqual(len(coll.instances), 3)
        self.assertEqual(len(pool._recovered[("asdf", "hjkl")]), 0)

    @gen_test
    async def test_returning_instances(self):
        region = "us-west-2"
        # Setup the AMI we need available to make instances
        conn = boto.ec2.connect_to_region(region)
        reservation = conn.run_instances('ami-1234abcd')
        instance = reservation.instances[0]
        conn.create_image(instance.id, "CoreOS stable")

        # Now run the test
        pool = self._callFUT("br12")
        await pool.ready
        pool.use_filters = True

        coll = await pool.request_instances("run_12", "12423", 5,
                                            inst_type="m1.small",
                                            region=region)
        await coll.wait_for_running()
        self.assertEqual(len(coll.instances), 5)

        # Return them
        await pool.release_instances(coll)
        self.assertEqual(len(pool._instances[region]), 5)

        # Acquire 5 again
        coll = await pool.request_instances("run_12", "42315", 5,
                                            inst_type="m1.small",
                                            region=region)
        self.assertEqual(len(coll.instances), 5)
        self.assertEqual(len(pool._instances[region]), 0)

    @gen_test
    async def test_reaping_all_instances(self):
        region = "us-west-2"
        # Setup the AMI we need available to make instances
        conn = boto.ec2.connect_to_region(region)
        reservation = conn.run_instances('ami-1234abcd')
        instance = reservation.instances[0]
        conn.create_image(instance.id, "CoreOS stable")

        # Now run the test
        pool = self._callFUT("br12")
        await pool.ready
        pool.use_filters = True

        coll = await pool.request_instances("run_12", "12423", 5,
                                            inst_type="m1.small",
                                            region=region)
        await coll.wait_for_running()
        self.assertEqual(len(coll.instances), 5)

        # Return them
        await pool.release_instances(coll)
        self.assertEqual(len(pool._instances[region]), 5)

        # Now, reap them
        await pool.reap_instances()
        self.assertEqual(len(pool._instances[region]), 0)
