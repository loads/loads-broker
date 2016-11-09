"""AWS Higher Level Abstractions

This module contains higher-level AWS abstractions to make working with
AWS instances and collections of instances easier and less error-prone.

:class:`EC2Instance` is responsible for maintaining information about
itself and updating its state when asked to. The executer passed in
must be capable of running functions that may block, ie a Greenlet or
ThreadPool executor.

:class:`EC2Collection` is a group of instances for a given allocation
request. Collections should be passed back to the Pool when their use
is no longer required.

An EC2 Pool is responsible for allocating and dispersing
:class:`EC2Instance's <EC2Instance>` and terminating idle instances.

The :class:`EC2Pool` is responsible for tracking EC2 instances across
regions, allocating them for use by the broker, and terminating
excessively idle instances. It also can rebuild maps of existing
instances by querying AWS for appropriate instance types.

"""
import concurrent.futures
import time
from collections import defaultdict, namedtuple
from datetime import datetime, timedelta

from boto.ec2 import connect_to_region
from tornado import gen
from tornado.concurrent import Future
from tornado.platform.asyncio import to_tornado_future
import tornado.ioloop

from loadsbroker.exceptions import LoadsException
from loadsbroker import logger


_POPULATED = False
AWS_REGIONS = (
    # "ap-northeast-1", "ap-southeast-1", "ap-southeast-2",  # speeding up
    "eu-west-1",
    # "sa-east-1",   # this one times out
    "us-east-1",
    "us-west-1",
    "us-west-2"
)


# Initial blank list of AMI ID's that will map a region to a dict keyed by
# virtualization type of the appropriate AMI to use
AWS_AMI_IDS = {k: {} for k in AWS_REGIONS}


def populate_ami_ids(aws_access_key_id=None, aws_secret_access_key=None,
                     port=None, owner_id="595879546273", use_filters=True):
    """Populate all the AMI ID's with the latest CoreOS stable info.

    This is a longer blocking operation and should be done on startup.
    """
    global _POPULATED

    # see https://github.com/boto/boto/issues/2617
    if port is not None:
        is_secure = port == 443
    else:
        is_secure = True

    # Spin up a temp thread pool to make this faster
    errors = []

    def get_amis(region):
        logger.debug("Working in %s" % region)
        try:
            conn = connect_to_region(
                region,
                aws_access_key_id=aws_access_key_id,
                aws_secret_access_key=aws_secret_access_key,
                port=port, is_secure=is_secure)

            filters = {}
            if owner_id is not None and use_filters:
                filters["owner-id"] = owner_id

            images = conn.get_all_images(filters=filters)

            # The last two highest sorted are the pvm and hvm instance id's
            # what is this 899.4 ??? XXX
            # images = sorted([x for x in images if "899.4" in x.name],
            #                key=lambda x: x.name)[-2:]
            images = sorted(images, key=lambda x: x.name)[-2:]
            AWS_AMI_IDS[region] = {x.virtualization_type: x for x in images}
            logger.debug("%s populated" % region)
        except Exception as exc:
            logger.exception('Could not get all images in %s' % region)
            errors.append(exc)

    with concurrent.futures.ThreadPoolExecutor(len(AWS_REGIONS)) as pool:
        # Execute all regions in parallel.
        pool.map(get_amis, AWS_REGIONS)

    if len(errors) > 0:
        raise errors[0]

    _POPULATED = True


def get_ami(region, instance_type):
    """Returns the appropriate AMI to use for a given region + instance type

    HVM is always used except for instance types which cannot use it. Based
    on matrix here:

    http://aws.amazon.com/amazon-linux-ami/instance-type-matrix/

    .. note::

        :func:`populate_ami_ids` must be called first to populate the available
        AMI's.

    """
    if not _POPULATED:
        raise KeyError('populate_ami_ids must be called first')

    instances = AWS_AMI_IDS[region]

    inst_type = "hvm"
    if instance_type[:2] in ["m1", "m2", "c1", "t1"]:
        inst_type = "paravirtual"

    if inst_type not in instances:
        msg = "Could not find instance type %r in %s for region %s"
        raise KeyError(msg % (inst_type, list(instances.keys()), region))

    return instances[inst_type].id


def available_instance(instance):
    """Returns True if an instance is usable for allocation.

    Instances are only usable if they're running, or have been
    "pending" for less than 2 minutes. Instances pending more than
    2 minutes are likely perpetually stalled and will be reaped.

    :type instance: :class:`instance.Instance`
    :returns: Whether the instance should be used for allocation.
    :rtype: bool

    """
    if instance.state == "running":
        return True

    if instance.state == "pending":
        oldest = datetime.today() - timedelta(minutes=2)
        try:
            launched = datetime.strptime(instance.launch_time,
                                         '%Y-%m-%dT%H:%M:%S.%fZ')
        except ValueError:
            # Trigger by moto tests as they don't include a timezone
            launched = datetime.strptime(instance.launch_time,
                                         '%Y-%m-%dT%H:%M:%S')
        if oldest < launched:
            return True

    return False


class ExtensionState:
    """A bare class that extensions can attach things to that will be
    retained on the instance."""
    pass


class EC2Instance(namedtuple('EC2Instance', 'instance state')):
    """EC2Instance that holds the underlying EC2.Instance object and
    configurable plugin state."""


class EC2Collection:
    """Create a collection to manage a set of instances.

    :type instances: list of :class:`instance.Instance`

    """
    def __init__(self, run_id, uuid, conn, instances, io_loop=None):
        self.run_id = run_id
        self.uuid = uuid
        self.started = False
        self.finished = False
        self.conn = conn
        self.local_dns = False
        self._env_data = None
        self._command_args = None
        self._executer = concurrent.futures.ThreadPoolExecutor(len(instances))
        self._loop = io_loop or tornado.ioloop.IOLoop.instance()

        self.instances = []
        for inst in instances:
            self.instances.append(EC2Instance(inst, ExtensionState()))

    def debug(self, msg):
        logger.debug('[uuid:%s] %s' % (self.uuid, msg))

    async def wait(self, seconds):
        """Waits for ``seconds`` before resuming."""
        await gen.Task(self._loop.add_timeout, time.time() + seconds)

    def execute(self, func, *args, **kwargs):
        """Execute a blocking function, return a future that will be
        called in the io loop.

        The blocking function will receive the underlying boto EC2
        instance object first, with the other args trailing.

        """
        fut = Future()

        def set_fut(future):
            exc = future.exception()
            if exc:
                fut.set_exception(exc)
            else:
                fut.set_result(future.result())

        def _throwback(fut):
            self._loop.add_callback(set_fut, fut)

        exc_fut = self._executer.submit(func, *args, **kwargs)
        exc_fut.add_done_callback(_throwback)
        return fut

    async def map(self, func, delay=0, *args, **kwargs):
        """Execute a blocking func with args/kwargs across all instances."""
        futures = []
        for x in self.instances:
            fut = self.execute(func, x, *args, **kwargs)
            futures.append(fut)
            if delay:
                await self.wait(delay)
        results = await gen.multi(futures)
        return results

    def pending_instances(self):
        return [i for i in self.instances if i.instance.state == "pending"]

    def dead_instances(self):
        return [i for i in self.instances
                if i.instance.state not in ["pending", "running"] or
                getattr(i.state, "nonresponsive", False)]

    def running_instances(self):
        return [i for i in self.instances if i.instance.state == "running"]

    async def remove_dead_instances(self):
        """Removes all dead instances per :meth:`dead_instances`."""
        dead = self.dead_instances()
        if dead:
            self.debug("Pruning %d non-responsive instances." % len(dead))
            await self.remove_instances(dead)

    async def wait_for_running(self, interval=5, timeout=600):
        """Wait for all the instances to be running. Instances unable
        to load will be removed."""
        def update_state(inst):
            try:
                inst.instance.update()
            except Exception:
                # Updating state can fail, it happens
                self.debug('Failed to update instance state: %s' %
                           inst.instance.id)
            return inst.instance.state

        end_time = time.time() + 600
        pending = self.pending_instances()

        while time.time() < end_time and pending:
            self.debug('%d pending instances.' % len(pending))
            # Update the state of all the pending instances
            await gen.multi(
                [self.execute(update_state, inst) for inst in pending])
            pending = self.pending_instances()

            # Wait if there's pending to check again
            if pending:
                await self.wait(interval)

        # Remove everything that isn't running by now
        dead = self.dead_instances() + self.pending_instances()

        # Don't wait for the future that kills them
        self.debug("Removing %d dead instances that wouldn't run" % len(dead))
        gen.convert_yielded(self.remove_instances(dead))
        return True

    async def remove_instances(self, ec2_instances):
        """Remove an instance entirely."""
        if not ec2_instances:
            return

        instances = [i.instance for i in ec2_instances]
        for inst in ec2_instances:
            self.instances.remove(inst)

        instance_ids = [x.id for x in instances]

        try:
            # Remove the tags
            await self.execute(self.conn.create_tags, instance_ids,
                               {"RunId": "", "Uuid": ""})
        except Exception:
            logger.debug("Error detagging instances, continuing.",
                         exc_info=True)

        try:
            logger.debug("Terminating instances %s" % str(instance_ids))
            # Nuke them
            await self.execute(self.conn.terminate_instances, instance_ids)
        except Exception:
            logger.debug("Error terminating instances.", exc_info=True)


class EC2Pool:
    """Initialize a pool for instance allocation and recycling.

    All instances allocated using this pool will be tagged as follows:

    Name
        loads-BROKER_ID
    Broker
        BROKER_ID

    Instances in use by a run are tagged with the additional tags:

    RunId
        RUN_ID
    Uuid
        STEP_ID

    .. warning::

        This instance is **NOT SAFE FOR CONCURRENT USE BY THREADS**.

    """
    def __init__(self, broker_id, access_key=None, secret_key=None,
                 key_pair="loads", security="loads", max_idle=600,
                 user_data=None, io_loop=None, port=None,
                 owner_id="595879546273", use_filters=True):
        self.owner_id = owner_id
        self.use_filters = use_filters
        self.broker_id = broker_id
        self.access_key = access_key
        self.secret_key = secret_key
        self.max_idle = max_idle
        self.key_pair = key_pair
        self.security = security
        self.user_data = user_data
        self._instances = defaultdict(list)
        self._tag_filters = {"tag:Name": "loads-%s*" % self.broker_id,
                             "tag:Project": "loads"}
        self._conns = {}
        self._recovered = {}
        self._executor = concurrent.futures.ThreadPoolExecutor(15)
        self._loop = io_loop or tornado.ioloop.IOLoop.instance()
        self.port = port
        # see https://github.com/boto/boto/issues/2617
        if port is not None:
            self.is_secure = port == 443
        else:
            self.is_secure = True

        # Asynchronously initialize ourself when the pool runs
        self._loop.add_future(
            gen.convert_yielded(self.initialize()),
            self._initialized
        )

        self.ready = Future()

    def shutdown(self):
        """Make sure we shutdown the executor.
        """
        self._executor.shutdown()

    def _run_in_executor(self, func, *args, **kwargs):
        return to_tornado_future(self._executor.submit(func, *args, **kwargs))

    def initialize(self):
        """Fully initialize the AWS pool and dependencies, recover existing
        instances, etc.

        :returns: A future that will require the loop running to retrieve.

        """
        logger.debug("Pulling CoreOS AMI info...")
        populate_ami_ids(self.access_key, self.secret_key, port=self.port,
                         owner_id=self.owner_id, use_filters=self.use_filters)
        return self._recover()

    def _initialized(self, future):
        # Run the result to ensure we raise an exception if any occurred
        logger.debug("Finished initializing: %s.", future.result())
        self.ready.set_result(True)

    async def _region_conn(self, region=None):
        if region in self._conns:
            return self._conns[region]

        # Setup a connection
        conn = await self._run_in_executor(
            connect_to_region, region,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            port=self.port, is_secure=self.is_secure)

        self._conns[region] = conn
        return conn

    async def _recover_region(self, region):
        """Recover all the instances in a region"""
        conn = await self._region_conn(region)

        if self.use_filters:
            filters = self._tag_filters
        else:
            filters = {}

        instances = await self._run_in_executor(
            conn.get_only_instances,
            filters=filters)

        return instances

    async def _recover(self):
        """Recover allocated instances from EC2."""
        recovered_instances = defaultdict(list)

        # Recover every region at once
        instancelist = await gen.multi(
            [self._recover_region(x) for x in AWS_REGIONS])

        logger.debug("Found %s instances to look at for recovery.",
                     sum(map(len, instancelist)))

        allocated = 0
        not_used = 0

        for instances in instancelist:
            for instance in instances:
                # skipping terminated instances
                if instance.state == 'terminated':
                    continue
                tags = instance.tags
                region = instance.region.name
                logger.debug('- %s (%s)' % (instance.id, region))
                # If this has been 'pending' too long, we put it in the main
                # instance pool for later reaping
                if not available_instance(instance):
                    self._instances[region].append(instance)
                    continue

                if tags.get("RunId") and tags.get("Uuid"):
                    # Put allocated instances into a recovery pool separate
                    # from unallocated
                    inst_key = (tags["RunId"], tags["Uuid"])
                    recovered_instances[inst_key].append(instance)
                    allocated += 1
                else:
                    self._instances[region].append(instance)
                    not_used += 1

        logger.debug("%d instances were allocated to a run" % allocated)
        logger.debug("%d instances were not used" % not_used)

        self._recovered = recovered_instances

    def _locate_recovered_instances(self, run_id, uuid):
        """Locates and removes existing allocated instances if any"""
        key = run_id, uuid

        if key not in self._recovered:
            # XXX do we want to raise here?
            return []

        instances = self._recovered[key]
        del self._recovered[key]
        return instances

    def _locate_existing_instances(self, count, inst_type, region):
        """Locates and removes existing available instances if any."""
        region_instances = self._instances[region]
        instances = []
        remaining = []

        for inst in region_instances:
            if available_instance(inst) and inst_type == inst.instance_type:
                    instances.append(inst)
            else:
                remaining.append(inst)

            if len(instances) >= count:
                break

        # Determine how many were removed, and reconstruct the unallocated
        # instance list with the instances not used
        removed = len(instances) + len(remaining)
        self._instances[region] = region_instances[removed:] + remaining
        return instances

    async def _allocate_instances(self, conn, count, inst_type, region):
        """Allocate a set of new instances and return them."""
        ami_id = get_ami(region, inst_type)
        reservations = await self._run_in_executor(
            conn.run_instances,
            ami_id, min_count=count, max_count=count,
            key_name=self.key_pair, security_groups=[self.security],
            user_data=self.user_data, instance_type=inst_type)

        return reservations.instances

    async def request_instances(self,
                                run_id,
                                uuid,
                                count=1,
                                inst_type="t1.micro",
                                region="us-west-2",
                                allocate_missing=True,
                                owner=None):
        """Allocate a collection of instances.

        :param run_id: Run ID for these instances
        :param uuid: UUID to use for this collection
        :param count: How many instances to allocate
        :param type: EC2 Instance type the instances should be
        :param region: EC2 region to allocate the instances in
        :param allocate_missing:
            If there's insufficient existing instances for this uuid,
            whether existing or new instances should be allocated to the
            collection.
        :param owner: str Owner name of the instances
        :returns: Collection of allocated instances
        :rtype: :class:`EC2Collection`

        """
        if region not in AWS_REGIONS:
            raise LoadsException("Unknown region: %s" % region)

        # First attempt to recover instances for this run/uuid
        instances = self._locate_recovered_instances(run_id, uuid)
        remaining_count = count - len(instances)

        conn = await self._region_conn(region)

        # If existing/new are not being allocated, the recovered are
        # already tagged, so we're done.
        if not allocate_missing:
            return EC2Collection(run_id, uuid, conn, instances, self._loop)

        # Add any more remaining that should be used
        instances.extend(
            self._locate_existing_instances(remaining_count, inst_type, region)
        )

        # Determine if we should allocate more instances
        num = count - len(instances)
        if num > 0:
            new_instances = await self._allocate_instances(
                conn, num, inst_type, region)
            logger.debug("Allocated instances%s: %s",
                         " (Owner: %s)" % owner if owner else "",
                         new_instances)
            instances.extend(new_instances)

        # Tag all the instances
        if self.use_filters:
            name = "loads-{}{}".format(
                self.broker_id, "-" + owner if owner else "")
            tags = {
                "Name": name,
                "Project": "loads",
                "RunId": run_id,
                "Uuid": uuid,
            }
            if owner:
                tags["Owner"] = owner

            # Sometimes, we can get instance data back before the AWS API fully
            # recognizes it, so we wait as needed.
            async def tag_instance(instance):
                retries = 0
                while True:
                    try:
                        await self._run_in_executor(
                            conn.create_tags, [instance.id], tags)
                        break
                    except:
                        if retries > 5:
                            raise
                    retries += 1
                    await gen.Task(self._loop.add_timeout, time.time() + 1)
            await gen.multi([tag_instance(x) for x in instances])
        return EC2Collection(run_id, uuid, conn, instances, self._loop)

    async def release_instances(self, collection):
        """Return a collection of instances to the pool.

        :param collection: Collection to return
        :type collection: :class:`EC2Collection`

        """
        # Sometimes a collection ends up with zero instances after pruning
        # dead ones
        if not collection.instances:
            return

        region = collection.instances[0].instance.region.name
        instances = [x.instance for x in collection.instances]

        # De-tag the Run data on these instances
        conn = await self._region_conn(region)

        if self.use_filters:
            await self._run_in_executor(
                conn.create_tags,
                [x.id for x in instances],
                {"RunId": "", "Uuid": ""})

        self._instances[region].extend(instances)

    async def reap_instances(self):
        """Immediately reap all instances."""
        # Remove all the instances before yielding actions
        all_instances = self._instances
        self._instances = defaultdict(list)

        for region, instances in all_instances.items():
            conn = await self._region_conn(region)

            # submit these instances for termination
            await self._run_in_executor(
                conn.terminate_instances,
                [x.id for x in instances])
