"""Broker Orchestration

The Broker is responsible for:

* Coordinating runs
* Ensuring run transitions
* Providing a rudimentary public API for use by the CLI/Web code

Complete name of environment variables available in a test container:

- BROKER_ID
    Unique ID of the broker running this test.
- BROKER_VERSION
    Version of the broker running the test.
- RUN_ID
    Unique run ID.
- CONTAINER_ID
    Container ID for the collection.
- HOST_IP
    The public IP of the host running the container.
- PRIVATE_IP
    The AWS internal IP of the host.
- STATSD_HOST
    IP of the statsd host to send metrics to.
- STATSD_PORT
    Port of the statsd host.

"""
import os
import time
import concurrent.futures
from collections import namedtuple
from datetime import datetime
from functools import partial

from sqlalchemy.orm.exc import NoResultFound
from tornado import gen
try:
    from influxdb.influxdb08 import InfluxDBClient
except ImportError:
    InfluxDBClient = None

from loadsbroker import logger, aws, __version__
from loadsbroker.db import (
    Database,
    Run,
    Project,
    RUNNING,
    TERMINATING,
    COMPLETED,
    setup_database,
)
from loadsbroker.exceptions import LoadsException
from loadsbroker.extensions import (
    DNSMasq,
    Docker,
    Heka,
    Ping,
    SSH,
    ContainerInfo,
)
from loadsbroker.util import dict2str
from loadsbroker.webapp.api import _DEFAULTS

import threading


BASE_ENV = dict(
    BROKER_VERSION=__version__,
)


HEKA_INFO = ContainerInfo(
    "kitcambridge/heka:0.8.1",
    "https://s3.amazonaws.com/loads-docker-images/heka-0.8.1.tar.bz2")

DNSMASQ_INFO = ContainerInfo(
    "kitcambridge/dnsmasq:latest",
    "https://s3.amazonaws.com/loads-docker-images/dnsmasq.tar.bz2")


def log_threadid(msg):
    """Log a message, including the thread ID"""
    thread_id = threading.currentThread().ident
    logger.debug("Msg: %s, ThreadID: %s", msg, thread_id)


class RunHelpers:
    """Empty object used to reference initialized extensions."""
    pass


class Broker:
    def __init__(self, io_loop, sqluri, ssh_key,
                 heka_options, influx_options, aws_port=None,
                 aws_owner_id="595879546273", aws_use_filters=True,
                 aws_access_key=None, aws_secret_key=None, initial_db=None):

        self.loop = io_loop
        self._base_env = BASE_ENV.copy()

        user_data = _DEFAULTS["user_data"]
        if user_data is not None and os.path.exists(user_data):
            with open(user_data) as f:
                user_data = f.read()

        self.influx_options = influx_options

        if influx_options is None:
            self.influx = None
        else:
            influx_args = {
                "host": influx_options.host,
                "port": influx_options.port,
                "username": influx_options.user,
                "password": influx_options.password,
                "database": "loads"
            }

            if influx_options.secure:
                influx_args["ssl"] = True
                influx_args["verify_ssl"] = True

            if InfluxDBClient is None:
                raise ImportError('You need to install the influx lib')
            self.influx = InfluxDBClient(**influx_args)

        self.pool = aws.EC2Pool("1234", user_data=user_data,
                                io_loop=self.loop, port=aws_port,
                                owner_id=aws_owner_id,
                                use_filters=aws_use_filters,
                                access_key=aws_access_key,
                                secret_key=aws_secret_key)

        # Utilities used by RunManager
        ssh = SSH(ssh_keyfile=ssh_key)
        self.run_helpers = run_helpers = RunHelpers()
        run_helpers.ping = Ping(self.loop)
        run_helpers.docker = Docker(ssh)
        run_helpers.dns = DNSMasq(DNSMASQ_INFO, run_helpers.docker)
        run_helpers.heka = Heka(HEKA_INFO, ssh=ssh, options=heka_options,
                                influx=influx_options)
        run_helpers.ssh = ssh

        self.db = Database(sqluri, echo=True)

        # Run managers keyed by uuid
        self._runs = {}

        # Ensure the db is setup
        if initial_db:
            setup_database(self.db.session(), initial_db)

    def shutdown(self):
        self.pool.shutdown()

    def get_projects(self, fields=None):
        projects = self.db.session().query(Project).all()
        return [proj.json(fields) for proj in projects]

    def get_project(self, project_id, fields=None):
        session = self.db.session()
        try:
            proj = session.query(Project).filter(
                Project.uuid == project_id).one()
        except NoResultFound:
            return None

        return proj.json(fields)

    def delete_project(self, project_id):
        session = self.db.session()
        try:
            proj = session.query(Project).filter(
                Project.uuid == project_id).one()
        except NoResultFound:
            return None

        session.delete(proj)
        session.commit()

    def get_runs(self, fields=None, limit=None, offset=None):
        # XXX filters
        log_threadid("Getting runs")
        runs = self.db.session().query(Run)
        if limit is not None:
            runs = runs.limit(limit)
        if offset is not None:
            runs = runs.offset(offset)
        return [run.json(fields) for run in runs]

    def _get_run(self, run_id):
        session = self.db.session()
        try:
            run = session.query(Run).filter(Run.uuid == run_id).one()
        except NoResultFound:
            run = None
        return run, session

    @gen.coroutine
    def _run_complete(self, session, mgr, future):
        try:
            response = yield future
            logger.debug("Got response of: %s", response)
        except:
            logger.error("Got an exception", exc_info=True)

    def abort_run(self, run_id):
        """Aborts a run."""
        if run_id not in self._runs:
            return False

        self._runs[run_id].abort = True
        return True

    def run_plan(self, strategy_id, create_db=True, **kwargs):
        session = self.db.session()

        log_threadid("Running strategy: %s" % strategy_id)
        uuid = kwargs.pop('run_uuid', None)

        # now we can start a new run
        try:
            mgr, future = RunManager.new_run(
                run_helpers=self.run_helpers,
                db_session=session,
                pool=self.pool,
                io_loop=self.loop,
                plan_uuid=strategy_id,
                run_uuid=uuid,
                additional_env=kwargs)
        except NoResultFound as e:
            raise LoadsException(str(e))

        callback = partial(self._run_complete, session, mgr)
        future.add_done_callback(callback)
        self._runs[mgr.run.uuid] = mgr

        # create an Influx Database
        if create_db:
            try:
                self._create_dbs(mgr.run.uuid)
            except:
                mgr.abort = True
                import pdb
                pdb.set_trace()
                raise

        return mgr.run.uuid

    def _create_dbs(self, run_id):
        if self.influx is None:
            return

        def create(name):
            return self.influx.create_database("db"+name.replace('-', ''))

        return self._db_action(run_id, create)

    def _delete_dbs(self, run_id):
        if self.influx is None:
            return

        def delete(name):
            return self.influx.drop_database("db"+name.replace('-', ''))

        return self._db_action(run_id, delete)

    def _db_action(self, run_id, action):
        names = [run_id]

        with concurrent.futures.ThreadPoolExecutor(len(names)) as e:
            results = e.map(action, names)

        return all(results)

    def delete_run(self, run_id):
        run, session = self._get_run(run_id)
        self._delete_dbs(run_id)
        session.delete(run)
        session.commit()
        # delete grafana


class StepRecordLink(namedtuple('StepRecordLink',
                                'step_record step ec2_collection')):
    """Named tuple that links a EC2Collection to the step and the actual
    step record."""


class RunManager:
    """Manages the life-cycle of a load run.

    """
    def __init__(self, run_helpers, db_session, pool, io_loop, run):
        self.helpers = run_helpers
        self.run = run
        self._db_session = db_session
        self._pool = pool
        self._loop = io_loop
        self._set_links = []
        self._dns_map = {}
        self.abort = False
        self._state_description = ""
        # XXX see what should be this time
        self.sleep_time = 1.5

        self.base_containers = [HEKA_INFO, DNSMASQ_INFO]

        # Setup the run environment vars
        self.run_env = BASE_ENV.copy()
        self.run_env["RUN_ID"] = str(self.run.uuid)

    def _set_state(self, state):
        self._state_description = state
        if state:
            logger.debug(state)

    def _get_state(self):
        return self._state_description

    state_description = property(_get_state, _set_state)

    @classmethod
    def new_run(cls, run_helpers, db_session, pool, io_loop, plan_uuid,
                run_uuid=None, additional_env=None):
        """Create a new run manager for the given strategy name

        This creates a new run for this strategy and initializes it.

        :param db_session: SQLAlchemy database session
        :param pool: AWS EC2Pool instance to allocate from
        :param io_loop: A tornado io loop
        :param plan_uuid: The strategy UUID to use for this run
        :param run_uuid: Use the provided run_uuid instead of generating one
        :param additional_env: Additional env args to use in container set
                               interpolation

        :returns: New RunManager in the process of being initialized,
                  along with a future tracking the run.

        """
        # Create the run for this manager
        run = Run.new_run(db_session, plan_uuid)
        if run_uuid:
            run.uuid = run_uuid
        db_session.add(run)
        db_session.commit()

        log_threadid("Committed new session.")

        run_manager = cls(run_helpers, db_session, pool, io_loop, run)
        if additional_env:
            run_manager.run_env.update(additional_env)
        future = run_manager.start()
        return run_manager, future

    @classmethod
    def recover_run(cls, run_uuid):
        """Given a run uuid, fully reconstruct the run manager state"""
        pass

    @property
    def uuid(self):
        return self.run.uuid

    @property
    def state(self):
        return self.run.state

    @gen.coroutine
    def _get_steps(self):
        """Request all the step instances needed from the pool

        This is a separate method as both the recover run and new run
        will need to run this identically.

        """
        logger.debug('Getting steps')
        steps = self.run.plan.steps
        collections = yield [
            self._pool.request_instances(self.run.uuid, s.uuid,
                                         count=s.instance_count,
                                         inst_type=s.instance_type,
                                         region=s.instance_region)
            for s in steps]

        try:
            # First, setup some dicst, all keyed by step.uuid
            steps_by_uuid = {x.uuid: x for x in steps}
            step_records_by_uuid = {x.step.uuid: x for x in
                                    self.run.step_records}

            # Link the step/step_record/ec2_collection under a single
            # StepRecordLink tuple
            for coll in collections:
                step = steps_by_uuid[coll.uuid]
                step_record = step_records_by_uuid[coll.uuid]
                setlink = StepRecordLink(step_record, step, coll)
                self._set_links.append(setlink)

        except Exception:
            # Ensure we return collections if something bad happened
            logger.error("Got an exception in runner, returning instances",
                         exc_info=True)

            try:
                yield [self._pool.release_instances(x) for x in collections]
            except:
                logger.error("Wat? Got an error returning instances.",
                             exc_info=True)

            # Clear out the setlinks to make sure they aren't cleaned up
            # again
            self._set_links = []

    @gen.coroutine
    def start(self):
        """Fully manage a complete run

        This doesn't return until the run is complete. A reference
        should be held so that the run state can be checked on as
        needed while this is running. This method chains to all the
        individual portions of a run.

        """
        try:
            # Initialize the run
            yield self._initialize()

            # Start and manage the run
            yield self._run()

            # Terminate the run
            yield self._shutdown()
        except:
            yield self._cleanup(exc=True)
        else:
            yield self._cleanup()

        return True

    @gen.coroutine
    def _initialize(self):
        # Initialize all the collections, this needs to always be done
        # just in case we're recovering
        yield self._get_steps()

        # Skip if we're running
        if self.state == RUNNING:
            return

        # Wait for the collections to come up
        self.state_description = "Waiting for running instances."
        yield [x.ec2_collection.wait_for_running() for x in self._set_links]

        # Setup docker on the collections
        docker = self.helpers.docker
        yield [docker.setup_collection(x.ec2_collection)
               for x in self._set_links]

        # Wait for docker on all the collections to come up
        self.state_description = "Waiting for docker"
        yield [docker.wait(x.ec2_collection, timeout=360)
               for x in self._set_links]

        logger.debug("Pulling base containers: heka")

        # Pull the base containers we need (for heka)
        self.state_description = "Pulling base container images"

        for container in self.base_containers:
            yield [docker.load_containers(x.ec2_collection, container.name,
                                          container.url) for x in
                   self._set_links]

        logger.debug("Pulling containers for this step.")
        # Pull the appropriate containers for every collection
        self.state_description = "Pulling step images"
        yield [docker.load_containers(x.ec2_collection, x.step.container_name,
                                      x.step.container_url) for x in
               self._set_links]

        self.state_description = ""

        self.run.state = RUNNING
        self.run.started_at = datetime.utcnow()
        self._db_session.commit()
        log_threadid("Now running.")

    @gen.coroutine
    def _shutdown(self):
        # If we aren't terminating, we shouldn't have been called
        if self.state != TERMINATING:
            return

        # Tell all the collections to shutdown
        yield [self._stop_step(s) for s in self._set_links]
        self.run.state = COMPLETED
        self.run.aborted = self.abort
        self._db_session.commit()

    @gen.coroutine
    def _cleanup(self, exc=False):
        if exc:
            # Ensure we try and shut them down
            logger.debug("Exception occurred, ensure containers terminated.",
                         exc_info=True)
            try:
                yield [self._stop_step(s) for s in self._set_links]
            except Exception:
                logger.error("Le sigh, error shutting down instances.",
                             exc_info=True)

        # Ensure we always release the collections we used
        logger.debug("Returning collections")

        try:
            yield [self._pool.release_instances(x.ec2_collection)
                   for x in self._set_links]
        except Exception:
            logger.error("Embarassing, error returning instances.",
                         exc_info=True)

        self._set_links = []

    @gen.coroutine
    def _run(self):
        # Skip if we're not running
        if self.state != RUNNING:
            return

        # Main run loop
        while True:
            if self.abort:
                logger.debug("Aborted, exiting run loop.")
                break

            stop = yield self._check_steps()
            if stop:
                break

            # Now we sleep for a bit
            yield gen.Task(self._loop.add_timeout, time.time() +
                           self.sleep_time)

        # We're done running, time to terminate
        self.run.state = TERMINATING
        self.run.completed_at = datetime.utcnow()
        self._db_session.commit()

    @gen.coroutine
    def _check_steps(self):
        """Checks steps for the plan to see if any existing steps
        have finished, or new ones need to start.

        When all the steps have run and completed, returns False
        to indicate nothing remains for the plan.

        """
        # Bools of collections that were started/finished
        started = [x.ec2_collection.started for x in self._set_links]
        finished = [x.ec2_collection.finished for x in self._set_links]

        # If all steps were started and finished, the run is complete.
        if all(started) and all(finished):
            return True

        # Locate all steps that have completed
        dones = yield [self._is_done(x) for x in self._set_links]
        dones = zip(dones, self._set_links)

        # Send shutdown to steps that have completed, we can shut them all
        # down in any order so we run in parallel
        @gen.coroutine
        def shutdown(setlink):
            try:
                yield self._stop_step(setlink)
            except:
                logger.error("Exception in shutdown.", exc_info=True)

            setlink.step_record.completed_at = datetime.utcnow()
            self._db_session.commit()
        yield [shutdown(s) for done, s in dones if done]

        # Start steps that should be started, ordered by delay
        starts = list(filter(self._should_start, self._set_links))
        starts.sort(key=lambda x: x.step.run_delay)

        # Start steps in order of lowest delay first, to ensure that steps
        # started afterwards can use DNS names/etc from prior steps
        for setlink in starts:
            # We tag the collection here since this may not actually run
            # until another time through this loop due to async nature
            setlink.ec2_collection.local_dns = bool(self._dns_map)

            try:
                yield self._start_step(setlink)
            except:
                logger.error("Exception starting.", exc_info=True)
                setlink.step_record.failed = True

            setlink.step_record.started_at = datetime.utcnow()
            self._db_session.commit()

            # If this collection reg's a dns name, add this collections
            # ip's to the name
            if setlink.step.dns_name:
                ips = [x.instance.ip_address for x
                       in setlink.ec2_collection.instances]
                self._dns_map[setlink.step.dns_name] = ips
        return False

    @gen.coroutine
    def _start_step(self, setlink):
        setlink.ec2_collection.started = True

        # Reload sysctl because coreos doesn't reload this right
        yield self.helpers.ssh.reload_sysctl(setlink.ec2_collection)

        # Start heka
        yield self.helpers.heka.start(setlink.ec2_collection,
                                      self.helpers.docker,
                                      self.helpers.ping,
                                      "db"+self.run.uuid.replace('-', ''),
                                      series=setlink.step.docker_series)

        # Startup local DNS if needed
        if setlink.ec2_collection.local_dns:
            logger.debug("Starting up DNS")
            yield self.helpers.dns.start(setlink.ec2_collection, self._dns_map)

        # Startup the testers
        env = "\n".join([dict2str(self.run_env),
                         setlink.step.environment_data,
                         "CONTAINER_ID=%s" % setlink.step.uuid])
        logger.debug("Starting step: %s", setlink.ec2_collection.uuid)
        yield self.helpers.docker.run_containers(
            setlink.ec2_collection,
            container_name=setlink.step.container_name,
            env=env,
            command_args=setlink.step.additional_command_args,
            ports=setlink.step.port_mapping or {},
            volumes=setlink.step.volume_mapping or {},
            delay=setlink.step.node_delay,
        )

    @gen.coroutine
    def _stop_step(self, setlink):
        # If we're already finished, don't shut things down twice
        if setlink.ec2_collection.finished:
            return

        setlink.ec2_collection.finished = True

        # Stop the docker testing agents
        yield self.helpers.docker.stop_containers(
            setlink.ec2_collection, setlink.step.container_name)

        # Stop heka
        yield self.helpers.heka.stop(setlink.ec2_collection,
                                     self.helpers.docker)

        # Stop dnsmasq
        if setlink.ec2_collection.local_dns:
            yield self.helpers.dns.stop(setlink.ec2_collection)

        # Remove anyone that failed to shutdown properly
        setlink.ec2_collection.remove_dead_instances()

    @gen.coroutine
    def _is_done(self, setlink):
        """Given a StepRecordLink, determine if the collection has
        finished or should be terminated."""
        # If we haven't been started, we can't be done
        if not setlink.step_record.started_at:
            return False

        # If we're already stopped, then we're obviously done
        if setlink.ec2_collection.finished:
            return True

        # If the collection has no instances running the container, its done
        docker = self.helpers.docker
        container_name = setlink.step.container_name
        instances_running = yield docker.is_running(
            setlink.ec2_collection,
            container_name,
            prune=setlink.step.prune_running
        )
        if not instances_running:
            logger.debug("No instances running, collection done.")
            return True

        # Remove instances that stopped responding
        yield setlink.ec2_collection.remove_dead_instances()

        # Otherwise return whether we should be stopped
        return setlink.step_record.should_stop()

    def _should_start(self, setlink):
        """Given a StepRecordLink, determine if the step should be started."""
        return setlink.step_record.should_start()
