"""AWS Higher Level Abstractions

This module contains higher-level AWS abstractions to make working with
AWS instances and collections of instances easier and less error-prone.

"""
import concurrent.futures
import time

from boto.ec2 import (
    connection,
    instance
)
from tornado import gen
import tornado.ioloop

from loadsbroker.exceptions import TimeoutException


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
        while True:
            yield self.update_state()

            if time.time() > end:
                raise TimeoutException()
            else if self.state != state:
                yield gen.Task(self._loop.add_timeout, time.time() + interval)


"""An AWS Collection is a group of instances for a given allocation
request

Collections should be passed back to the Pool when their use is no longer
required.

"""
class EC2Collection:
    """Create a collection to manage a set of instances.

    :type instances: list of :ref:`instance.Instance`

    """
    def __init__(self, conn, instances, io_loop=None):
        self._executer = concurrent.futures.ThreadPoolExecutor(len(instances))
        self._loop = io_loop or tornado.ioloop.IOLoop.instance()

        self._instances = []
        for inst in instances:
            ec2inst = EC2Instance(inst, conn, self._executer, self._loop)
            self._instances.append(ec2inst)


"""An AWS EC2 Pool is responsible for allocating and dispersing
:ref:`EC2Instance`s and terminating idle instances.

The AWS EC2 Pool is responsible for tracking EC2 instances across
regions, allocating them for use by the broker, and terminating
excessively idle instances. It also can rebuild maps of existing
instances by querying AWS for appropriate instance types.

"""
class EC2Pool:
    """Initialize a pool for instance allocation and recycling."""
    def __init__(self, access_key=None, secret_key=None, max_idle=600):
        pass

    """Allocate a collection of instances.

    :param count: How many instances to allocate
    :param type: EC2 Instance type the instances should be
    :param region: EC2 region to allocate the instances in
    :returns: Collection of allocated instances
    :rtype: :ref:`EC2Collection`

    """
    def request_instances(count=1, type="m3.large", region="us-west-2"):
        pass

    """Return a collection of instances to the pool.

    :param collection: Collection to return
    :type collection: :ref:`EC2Collection`

    """
    def return_instances(collection):
        pass