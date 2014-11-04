"""Broker Orchestration

The Broker is responsible for:

* Coordinating runs
* Ensuring run transitions
* Providing a rudimentary public API for use by the CLI/Web clients

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
    The IP of the host running the container.
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
from influxdb import InfluxDBClient

from loadsbroker import logger, aws, __version__
from loadsbroker.util import dict2str
from loadsbroker.dockerctrl import DockerDaemon
# XXX move that?
from loadsbroker.db import (
    Database,
    Run,
    Strategy,
    ContainerSet,
    RUNNING,
    TERMINATING,
    COMPLETED,
    status_to_text,
    setup_database,
)
from loadsbroker.extensions import (
    CAdvisor,
    DNSMasq,
    Docker,
    Heka,
    Ping,
    SSH,
)
from loadsbroker.webapp.api import _DEFAULTS

import threading


BASE_ENV = dict(
    BROKER_VERSION=__version__,
)


def log_threadid(msg):
    thread_id = threading.currentThread().ident
    logger.debug("Msg: %s, ThreadID: %s", msg, thread_id)


class RunHelpers:
    pass


class Broker:
    def __init__(self, io_loop, sqluri, ssh_key, ssh_username,
                 heka_options, influx_options, aws_port=None,
                 aws_owner_id="595879546273", aws_use_filters=True,
                 aws_access_key=None, aws_secret_key=None):

        self.loop = io_loop
        self._base_env = BASE_ENV.copy()

        user_data = _DEFAULTS["user_data"]
        if user_data is not None and os.path.exists(user_data):
            with open(user_data) as f:
                user_data = f.read()

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
        run_helpers.cadvisor = CAdvisor(influx_options)
        run_helpers.ping = Ping(self.loop)
        run_helpers.docker = Docker(ssh)
        run_helpers.dns = DNSMasq(run_helpers.docker)
        run_helpers.heka = Heka(ssh=ssh, options=heka_options)

        self.db = Database(sqluri, echo=True)
        self.sqluri = sqluri
        self.ssh_key = ssh_key
        self.ssh_username = ssh_username
        self._local_docker = DockerDaemon(host="tcp://0.0.0.0:2375")

        # Run managers keyed by uuid
        self._runs = {}

    def shutdown(self):
        self.pool.shutdown()
        self._print_status()

    @gen.coroutine
    def _print_status(self):
        while True:
            if not len(self._runs):
                logger.debug("Status: No runs in progress.")
            for uuid, mgr in self._runs.items():
                run = mgr.run
                logger.debug("Run state for %s: %s - %s", run.uuid,
                             status_to_text(mgr.state), mgr.state_description)
            yield gen.Task(self.loop.add_timeout, time.time() + 10)

    def get_runs(self, fields=None):
        # XXX filters, batching
        log_threadid("Getting runs")
        runs = self.db.session().query(Run).all()
        return [run.json(fields) for run in runs]

    def _get_run(self, run_id):
        session = self.db.session()
        try:
            run = session.query(Run).filter(Run.uuid == run_id).one()
        except NoResultFound:
            run = None
        return run, session

    @gen.coroutine
    def _test(self, session, mgr, future):
        try:
            response = yield future
            logger.debug("Got response of: %s", response)
        except:
            logger.error("Got an exception", exc_info=True)

        # logger.debug("Reaping the pool")
        # yield self.pool.reap_instances()
        # logger.debug("Finished terminating.")

    def run_test(self, **options):
        session = self.db.session()

        strategy_name = options.get('strategy_name', 'strategic!')

        # Ensure the db is setup
        setup_database(session, **options)

        log_threadid("Run_test")

        # now we can start a new run
        mgr, future = RunManager.new_run(self.run_helpers, session, self.pool,
                                         self.loop, strategy_name)

        callback = partial(self._test, session, mgr)
        future.add_done_callback(callback)
        self._runs[mgr.run.uuid] = mgr

        # create an Influx Database
        # self._create_dbs(mgr.run.uuid)

        # and start a Grafana container for our run
        # self._start_grafana(mgr.run.uuid)

        return mgr.run.uuid

    def _create_dbs(self, run_id):
        names = [run_id, "%s-cadvisor" % run_id]

        def create_database(name):
            return self.influx.create_database(name)

        with concurrent.futures.ThreadPoolExecutor(len(names)) as e:
            results = e.map(create_database, names)

        return all(results)

    @gen.coroutine
    def _start_grafana(self, run_id):
        environment = {'HTTP_USER': 'admin',
                       'HTTP_PASS': 'admin',
                       'INFLUXDB_HOST': 'localhost',
                       'INFLUXDB_NAME': run_id}
        ports = [80]

        # XXX we want one port per grafana and let the broker
        # hold a mapping {run_id: grafana port}
        # so we can display the dashboard link
        port_bindings = {80: 8088}

        result = self._executer.submit(self._local_docker.run_container,
                                       'tutum/grafana',
                                       environment=environment,
                                       ports=ports)
        container = result["Id"]
        self._local_docker.start(container, port_bindings=port_bindings)
        yield container

    def delete_run(self, run_id):
        run, session = self._get_run(run_id)
        self.influx.delete_database(run_id)
        session.delete(run)
        session.commit()
        # delete grafana


class ContainerSetLink(namedtuple('ContainerSetLink',
                                  'running meta collection')):
    """Named tuple that links a EC2Collection to the metadata
    describing its container running info and the running db instance
    of it."""


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
        self._use_dns = False
        self._dns_map = {}
        self.abort = False
        self.state_description = ""
        # XXX see what should be this time
        self.sleep_time = .1

        self.base_containers = [("kitcambridge/heka:dev", None),
                                ("google/cadvisor:latest", None),
                                ("kitcambridge/dnsmasq:latest", None)]

        # Setup the run environment vars
        self.run_env = BASE_ENV.copy()
        self.run_env["RUN_ID"] = str(self.run.id)

    @classmethod
    def new_run(cls, run_helpers, db_session, pool, io_loop, strategy_name):
        """Create a new run manager for the given strategy name

        This creates a new run for this strategy and initializes it.

        :param db_session: SQLAlchemy database session
        :param pool: AWS EC2Pool instance to allocate from
        :param io_loop: A tornado io loop
        :param strategy_name: The strategy name to use for this run

        :returns: New RunManager in the process of being initialized,
                  along with a future tracking the run.

        """
        # Create the run for this manager
        run = Run.new_run(db_session, strategy_name)
        db_session.add(run)
        db_session.commit()

        log_threadid("Committed new session.")

        run_manager = cls(run_helpers, db_session, pool, io_loop, run)
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
    def _get_container_sets(self):
        """Request all the container sets instances needed from the pool

        This is a separate method as both the recover run and new run
        will need to run this identically.

        """
        csets = self.run.strategy.container_sets
        collections = yield [
            self._pool.request_instances(self.run.uuid, c.uuid,
                                         count=c.instance_count,
                                         inst_type=c.instance_type,
                                         region=c.instance_region)
            for c in csets]

        try:
            # Setup the collection lookup info
            coll_by_uuid = {x.uuid: x for x in csets}
            running_by_uuid = {x.container_set.uuid: x
                               for x in self.run.running_container_sets}
            for coll in collections:
                meta = coll_by_uuid[coll.uuid]
                running = running_by_uuid[coll.uuid]
                setlink = ContainerSetLink(running, meta, coll)
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
        yield self._get_container_sets()

        # Skip if we're running
        if self.state == RUNNING:
            return

        # Wait for the collections to come up
        self.state_Description = "Waiting for running instances."
        yield [x.collection.wait_for_running() for x in self._set_links]

        # Setup docker on the collections
        docker = self.helpers.docker
        yield [docker.setup_collection(x.collection) for x in self._set_links]

        # Wait for docker on all the collections to come up
        self.state_description = "Waiting for docker"
        yield [docker.wait(x.collection, timeout=120) for x in self._set_links]

        logger.debug("Pulling base containers: heka/cadvisor")

        # Pull the base containers we need (for heka / cadvisor)
        self.state_description = "Pulling base container images"

        for container_name, container_url in self.base_containers:
            yield [docker.load_containers(x.collection, container_name,
                                          container_url) for x in
                   self._set_links]

        logger.debug("Pulling containers for this set.")
        # Pull the appropriate containers for every collection
        self.state_description = "Pulling container set images"
        yield [docker.load_containers(x.collection, x.meta.container_name,
                                      x.meta.container_url) for x in
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
        yield [self._stop_set(s) for s in self._set_links]
        self.run.state = COMPLETED
        self._db_session.commit()

    @gen.coroutine
    def _cleanup(self, exc=False):
        if exc:
            # Ensure we try and shut them down
            logger.debug("Exception occurred, ensure containers terminated.",
                         exc_info=True)
            try:
                yield [self._stop_set(s) for s in self._set_links]
            except Exception:
                logger.error("Le sigh, error shutting down instances.",
                             exc_info=True)

        # Ensure we always release the collections we used
        logger.debug("Returning collections")

        try:
            yield [self._pool.release_instances(x.collection)
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

        while True:
            if self.abort:
                logger.debug("Aborted, exiting run loop.")
                break

            stop = yield self._check_containers()
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
    def _check_containers(self):
        # If we have a dns map, we can use dns now
        # This is done at the top to ensure prior containers had a
        # chance to run first
        if self._dns_map:
            self._use_dns = True

        # First, only consider collections not completed
        running_collections = [x for x in self._set_links
                               if not x.running.completed_at]

        # Bools of collections that were started
        started = [x.collection.started for x in running_collections]

        # Bools of collections that are done
        dones = yield [self._is_done(x) for x in running_collections]

        # Send shutdown to all finished collections, we're not going
        # to wait on these futures, they will save when they complete
        for done, setlink in zip(dones, self._set_links):
            if not done or setlink.collection.finished:
                continue

            future = self._stop_set(setlink)
            future.add_done_callback(partial(self._stopped, setlink))

        # If all have started and are done, the run is complete.
        if all(dones) and all(started):
            return True

        # Locate container sets that need to be started
        starts = yield [self._should_start(x) for x in self._set_links]

        # Startup containers that should be started
        for start, setlink in zip(starts, self._set_links):
            if not start or setlink.collection.started:
                continue

            setlink.collection.local_dns = self._use_dns
            future = self._start_set(setlink)
            future.add_done_callback(partial(self._started, setlink))

            # If this collection reg's a dns name, add this collections
            # ip's to the name
            if setlink.meta.dns_name:
                ips = [x.instance.ip_address for x
                       in setlink.collection.instances]
                self._dns_map[setlink.meta.dns_name] = ips

        return False

    @gen.coroutine
    def _start_set(self, setlink):
        if setlink.collection.started:
            return
        setlink.collection.started = True

        # Start cadvisor
        database_name = "%s-cadvisor" % self.run.id
        logger.debug("Starting up cadvisor on the hosts")
        yield self.helpers.cadvisor.start(
            setlink.collection, self.helpers.docker, self.helpers.ping,
            database_name)

        # Start heka
        yield self.helpers.heka.start(setlink.collection,
                                      self.helpers.docker,
                                      self.helpers.ping)

        # Startup local DNS if needed
        if self._use_dns:
            logger.debug("Starting up DNS")
            yield self.helpers.dns.start(setlink.collection, self._dns_map)

        # Startup the testers
        env = "\n".join([dict2str(self.run_env),
                         setlink.meta.environment_data,
                         "CONTAINER_ID=%s" % setlink.meta.uuid])
        logger.debug("Starting container set: %s", setlink.collection.uuid)
        yield self.helpers.docker.run_containers(
            setlink.collection,
            container_name=setlink.meta.container_name,
            env=env,
            command_args=setlink.meta.additional_command_args,
            ports=setlink.meta.port_mapping or {}
        )

    @gen.coroutine
    def _stop_set(self, setlink):
        if setlink.collection.finished:
            return
        setlink.collection.finished = True

        # Stop the docker testing agents
        yield self.helpers.docker.stop_containers(
            setlink.collection, setlink.meta.container_name)

        # Stop heka
        yield self.helpers.heka.stop(setlink.collection, self.helpers.docker)

        # Stop cadvisor
        yield self.helpers.cadvisor.stop(setlink.collection,
                                         self.helpers.docker)

        # Stop dnsmasq
        if self._use_dns:
            yield self.helpers.dns.stop(setlink.collection)

    def _stopped(self, setlink, fut):
        """Runs after a setlink has stopped."""
        setlink.running.completed_at = datetime.utcnow()
        self._db_session.commit()

        try:
            fut.result()
        except:
            logger.error("Exception in shutdown.", exc_info=True)

    def _started(self, setlink, fut):
        """Runs after a setlink has started."""
        setlink.collection.started = True
        setlink.running.started_at = datetime.utcnow()
        self._db_session.commit()
        try:
            fut.result()
        except:
            logger.error("Exception starting.", exc_info=True)

    @gen.coroutine
    def _is_done(self, setlink):
        """Given a ContainerSetLink, determine if the collection has
        finished or should be terminated."""
        # If we haven't been started, we can't be done
        if not setlink.running.started_at:
            return False

        # If the collection has no instances running the container, its done
        docker = self.helpers.docker
        container_name = setlink.meta.container_name
        instances_running = yield docker.is_running(setlink.collection,
                                                    container_name)
        if not instances_running:
            logger.debug("No instances running, collection done.")
            return True

        # Otherwise return whether we should be stopped
        return setlink.running.should_stop()

    @gen.coroutine
    def _should_start(self, setlink):
        """Given a ContainerSetLink, determine if the collection should
         be started."""
        # If the collection is already running, this is a moot point since
        # we can't start it again
        if setlink.collection.started:
            return False

        # If we've waited longer than the delay
        return setlink.running.should_start()
