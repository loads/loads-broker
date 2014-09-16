"""AWS Higher Level Abstractions

This module contains higher-level AWS abstractions to make working with
AWS instances and collections of instances easier and less error-prone.

"""
import concurrent.futures
import time
from collections import defaultdict

from boto.ec2 import (
    connect_to_region,
    connection,
    instance
)
from tornado import gen
import tornado.ioloop

from loadsbroker.dockerctrl import DockerDaemon
from loadsbroker.exceptions import (
    LoadsException,
    TimeoutException
)
from loadsbroker import logger

AWS_REGIONS = [
    "ap-northeast-1", "ap-southeast-1", "ap-southeast-2",
    "eu-west-1",
    "sa-east-1",
    "us-east-1", "us-west-1", "us-west-2"
]

# Initial blank list of AMI ID's that will map a region to a dict keyed by
# virtualization type of the appropriate AMI to use
AWS_AMI_IDS = {k: {} for k in AWS_REGIONS}


"""Populate all the AMI ID's with the latest CoreOS stable info.

This is a longer blocking operation and should be done on startup.

"""
def populate_ami_ids(aws_access_key_id=None, aws_secret_access_key=None):
    for region in AWS_REGIONS:
        conn = connect_to_region(region, aws_access_key_id=aws_access_key_id,
                                 aws_secret_access_key=aws_secret_access_key)
        images = conn.get_all_images(filters={"owner-id": "595879546273"})

        # The last two highest sorted are the pvm and hvm instance id's
        images = sorted([x for x in images if "stable" in x.name],
                        key=lambda x: x.name)[-2:]
        AWS_AMI_IDS[region] = {x.virtualization_type: x for x in images}


"""Returns the appropriate AMI to use for a given region + instance type

HVM is always used except for instance types which cannot use it. Based
on matrix here: http://aws.amazon.com/amazon-linux-ami/instance-type-matrix/

.. note::

    :ref:`populate_ami_ids` must be called first to populate the available
    AMI's.

"""
def get_ami(region, instance_type):
    instances = AWS_AMI_IDS[region]

    inst_type = "hvm"
    if instance_type[:2] in ["m1", "m2", "c1", "t1"]:
        inst_type = "paravirtual"

    return instances[inst_type].id


"""An AWS instance is responsible for maintaining information about
itself and updating its state when asked to."""
class EC2Instance:
    """Create an instance.

    The executer passed in must be capable of running functions that
    may block, ie a Greenlet or ThreadPool executor.

    :type instance: :ref:`instance.Instance`
    :type conn: :ref:`connection.EC2Connection`
    :type executer: :ref:`concurrent.futures.Executor`

    """
    def __init__(self, instance, conn, executer, io_loop=None):
        self.state = instance.state
        self.type = instance.instance_type
        self._instance = instance
        self._executer = executer
        self._docker = None
        self._loop = io_loop or tornado.ioloop.IOLoop.instance()

    """Updates the state of this instance."""
    @gen.coroutine
    def update_state(self):
        self.state = yield self._executer.submit(self._instance.update)

    """Continually updates the state until the target state is reached
    or the timeout is hit.

    Defaults to a time-out of 10 minutes with 5 seconds between each
    check.

    :raises:
        :exc: `TimeoutException` if timeout is exceeded without the
              state change occurring.

    """
    @gen.coroutine
    def wait_for_state(self, state, interval=5, timeout=600):
        if self.state == state:
            return

        end = time.time() + timeout
        while self.state != state:
            if time.time() > end:
                raise TimeoutException()

            yield self.update_state()

            if self.state != state:
                yield gen.Task(self._loop.add_timeout, time.time() + interval)

    """Waits till docker is available on the host"""
    @gen.coroutine
    def wait_for_docker(self, interval=5, timeout=600):
        end = time.time() + timeout

        # First, wait till we're running
        yield self.wait_for_state("running")

        # Ensure we have a docker daemon for ourself
        if not self._docker:
            self._docker = DockerDaemon(
                host="tcp://%s:2375" % self._instance.ip_address)

        # Attempt to fetch until it works
        success = False
        while not success:
            try:
                containers = yield self._executer.submit(
                    self._docker.get_containers)
                success = True
            except Exception as e:
                # Wait 5 seconds to try again
                yield gen.Task(self._loop.add_timeout, time.time() + interval)

                if time.time() > end:
                    raise TimeoutException()


"""An AWS Collection is a group of instances for a given allocation
request

Collections should be passed back to the Pool when their use is no longer
required.

"""
class EC2Collection:
    """Create a collection to manage a set of instances.

    :type instances: list of :ref:`instance.Instance`

    """
    def __init__(self, run_id, conn, instances, io_loop=None):
        self._run_id = run_id
        self._executer = concurrent.futures.ThreadPoolExecutor(len(instances))
        self._loop = io_loop or tornado.ioloop.IOLoop.instance()

        self._instances = []
        for inst in instances:
            ec2inst = EC2Instance(inst, conn, self._executer, self._loop)
            self._instances.append(ec2inst)

    """Wait till all the instances are ready for docker commands"""
    @gen.coroutine
    def wait_for_docker(self):
        yield [inst.wait_for_docker() for inst in self._instances]

"""An AWS EC2 Pool is responsible for allocating and dispersing
:ref:`EC2Instance`s and terminating idle instances.

The AWS EC2 Pool is responsible for tracking EC2 instances across
regions, allocating them for use by the broker, and terminating
excessively idle instances. It also can rebuild maps of existing
instances by querying AWS for appropriate instance types.

"""
class EC2Pool:
    """Initialize a pool for instance allocation and recycling.

    All instances allocated using this pool will be tagged as follows:

    Name
        loads-BROKER_ID
    Broker
        BROKER_ID
    Run (if this instance is currently associate with a Run)
        RUN_ID

    .. warning::

        This instance is **NOT SAFE FOR CONCURRENT USE BY THREADS**.

    """
    def __init__(self, broker_id, access_key=None, secret_key=None,
                 key_pair="loads", security="loads",max_idle=600,
                 user_data=None, io_loop=None):
        self.broker_id = broker_id
        self.access_key = access_key
        self.secret_key = secret_key
        self.max_idle = max_idle
        self.key_pair = key_pair
        self.security = security
        self.user_data = user_data
        self._instances = defaultdict(lambda: [])
        self._conns = {}
        self._executor = concurrent.futures.ThreadPoolExecutor(15)
        self._loop = io_loop or tornado.ioloop.IOLoop.instance()

    @gen.coroutine
    def _region_conn(self, region=None):
        if region in self._conns:
            return self._conns[region]

        # Setup a connection
        logger.debug("requesting new connection")
        conn = yield self._executor.submit(connect_to_region,
            region, aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key)
        self._conns[region] = conn
        logger.debug("returning new connection")
        return conn

    """Recover allocated instances from EC2"""
    def recover(self):
        pass

    """Internal function that locates and removes instances if any"""
    def _locate_existing_instances(self, count, inst_type, region):
        region_instances = self._instances[region]
        instances = []
        remaining = []
        for inst in region_instances:
            if inst.state in ["running", "pending"] and \
                    inst.instance_type == inst_type:
                instances.append(instances)
            else:
                remaining.append(inst)

            if len(instances) > count:
                break

        # Determine how many were removed, and reconstruct the unallocated
        # instance list with the instances not used
        removed = len(instances) + len(remaining)
        self._instances[region] = region_instances[removed:] + remaining
        return instances

    """Allocate a set of new instances and return them"""
    @gen.coroutine
    def _allocate_instances(self, conn, count, inst_type, region):
        ami_id = get_ami(region, inst_type)
        reservations = yield self._executor.submit(conn.run_instances,
            ami_id, min_count=count, max_count=count,
            key_name=self.key_pair, security_groups=[self.security],
            user_data=self.user_data, instance_type=inst_type)
        return reservations.instances

    """Allocate a collection of instances.

    :param run_id: Run ID for these instances
    :param count: How many instances to allocate
    :param type: EC2 Instance type the instances should be
    :param region: EC2 region to allocate the instances in
    :returns: Collection of allocated instances
    :rtype: :ref:`EC2Collection`

    """
    @gen.coroutine
    def request_instances(self, run_id, count=1, inst_type="t1.micro",
                          region="us-west-2"):
        if region not in AWS_REGIONS:
            raise LoadsException("Unknown region: %s" % region)

        instances = self._locate_existing_instances(count, inst_type, region)
        conn = yield self._region_conn(region)

        num = count - len(instances)
        if num > 0:
            new_instances = yield self._allocate_instances(
                conn, num, inst_type, region)
            logger.debug("Allocated instances: %s", new_instances)
            instances.extend(new_instances)

        # Tag all the instances
        yield self._executor.submit(
            conn.create_tags,
            [x.id for x in instances],
            {
                "Name": "loads-%s" % self.broker_id,
                "Project": "loads",
                "RunId": run_id
            }
        )
        return EC2Collection(run_id, conn, instances, self._loop)

    """Return a collection of instances to the pool.

    :param collection: Collection to return
    :type collection: :ref:`EC2Collection`

    """
    @gen.coroutine
    def return_instances(self, collection):
        instance = collection._instances[0]._instance
        region = instance.region.name
        instances = [x._instance for x in collection._instances]

        # De-tag the Run data on these instances
        conn = yield self._region_conn(region)

        yield self._executor.submit(conn.create_tags,
            [x.id for x in instances],
            {"RunId": ""})

        self._instances[region].extend(instances)


    """Immediately reap all instances"""
    @gen.coroutine
    def reap_instances(self):
        # Remove all the instances before yielding actions
        all_instances = self._instances
        self._instances = defaultdict(lambda: [])

        for region, instances in all_instances.items():
            conn = yield self._region_conn(region)

            # submit these instances for termination
            yield self._executor.submit(conn.terminate_instances,
                [x.id for x in instances])
