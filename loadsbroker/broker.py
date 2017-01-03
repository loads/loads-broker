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
from datetime import datetime
from functools import partial

from sqlalchemy.orm.exc import NoResultFound
from tornado import gen

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
    InfluxDB,
    Watcher,
    Ping,
    SSH,
)
from loadsbroker.lifetime import (
    DNSMASQ_INFO,
    HEKA_INFO,
    INFLUXDB_INFO,
    WATCHER_INFO,
    InfluxDBStepRecordLink
)
from loadsbroker.options import InfluxDBOptions
from loadsbroker.webapp.api import _DEFAULTS

import threading


BASE_ENV = dict(
    BROKER_VERSION=__version__,
)


def log_threadid(msg):
    """Log a message, including the thread ID"""
    thread_id = threading.currentThread().ident
    logger.debug("Msg: %s, ThreadID: %s", msg, thread_id)


class RunHelpers:
    """Empty object used to reference initialized extensions."""
    pass


class Broker:
    def __init__(self, name, io_loop, sqluri, ssh_key,
                 heka_options, aws_port=None,
                 aws_owner_id="595879546273", aws_use_filters=True,
                 aws_access_key=None, aws_secret_key=None, initial_db=None):
        self.name = name
        logger.info("Starting loads-broker (%s)", self.name)

        self.loop = io_loop
        self._base_env = BASE_ENV.copy()
        aws_creds = {'AWS_ACCESS_KEY_ID': aws_access_key,
                     'AWS_SECRET_ACCESS_KEY': aws_secret_key}
        user_data = _DEFAULTS["user_data"]
        if user_data is not None and os.path.exists(user_data):
            with open(user_data) as f:
                user_data = f.read()

        logger.debug('Initializing AWS EC2 Pool')
        self.pool = aws.EC2Pool(self.name, user_data=user_data,
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
        run_helpers.heka = Heka(HEKA_INFO, ssh=ssh, options=heka_options)
        run_helpers.watcher = Watcher(WATCHER_INFO, options=aws_creds)
        run_helpers.influxdb = InfluxDB(INFLUXDB_INFO, ssh,
                                        aws_creds=aws_creds)
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

    def _run_complete(self, session, mgr, future):
        logger.debug('Run Plan completed')
        try:
            response = future.result()
            logger.debug("Run response of: %s", response)
        except:
            logger.error("Run did an exception", exc_info=True)

    def abort_run(self, run_id):
        """Aborts a run."""
        if run_id not in self._runs:
            return False

        self._runs[run_id].abort = True
        return True

    def run_plan(self, strategy_id, **kwargs):
        session = self.db.session()

        log_threadid("Running strategy: %s" % strategy_id)
        uuid = kwargs.pop('run_uuid', None)
        owner = kwargs.pop('owner', None)

        # now we can start a new run
        try:
            mgr, future = RunManager.new_run(
                run_helpers=self.run_helpers,
                db_session=session,
                pool=self.pool,
                io_loop=self.loop,
                plan_uuid=strategy_id,
                run_uuid=uuid,
                additional_env=kwargs,
                owner=owner)
        except NoResultFound as e:
            raise LoadsException(str(e))

        callback = partial(self._run_complete, session, mgr)
        future.add_done_callback(callback)
        self._runs[mgr.run.uuid] = mgr
        return mgr.run.uuid

    def delete_run(self, run_id):
        run, session = self._get_run(run_id)
        session.delete(run)
        session.commit()
        # delete grafana


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

    def _set_state(self, state):
        self._state_description = state
        if state:
            logger.debug(state)

    def _get_state(self):
        return self._state_description

    state_description = property(_get_state, _set_state)

    @classmethod
    def new_run(cls, run_helpers, db_session, pool, io_loop, plan_uuid,
                run_uuid=None, additional_env=None, owner=None):
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
        logger.debug('Starting a new run manager')
        run = Run.new_run(db_session, plan_uuid, owner)
        if run_uuid:
            run.uuid = run_uuid
        run.environment_data = env = BASE_ENV.copy()
        env['RUN_ID'] = str(run.uuid)
        env.update(additional_env)
        db_session.add(run)
        db_session.commit()

        log_threadid("Committed new session.")

        run_manager = cls(run_helpers, db_session, pool, io_loop, run)
        future = gen.convert_yielded(run_manager.start())
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

    @property
    def influxdb_options(self) -> InfluxDBOptions:
        """Return managed InfluxDB options for the current run"""
        for set_link in self._set_links:
            if isinstance(set_link, InfluxDBStepRecordLink):
                # assume 1
                instance = set_link.ec2_collection.instances[0].instance
                # XXX: better dbname? e.g. if Run adopts a user
                # provided SHA1
                dbname = "loads" + self.run.uuid.replace('-', '')
                return InfluxDBOptions(
                    instance.ip_address, 8086, None, None, dbname, False)

    async def _get_steps(self):
        """Request all the step instances needed from the pool

        This is a separate method as both the recover run and new run
        will need to run this identically.

        """
        logger.debug('Getting steps & collections')
        steps = self.run.plan.steps
        collections = await gen.multi(
            [self._pool.request_instances(
                self.run.uuid,
                s.uuid,
                count=s.instance_count,
                inst_type=s.instance_type,
                region=s.instance_region,
                plan=self.run.plan.name,
                owner=self.run.owner,
                run_max_time=s.run_delay + s.run_max_time)
             for s in steps])

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
                setlink = step.link(step_record, coll)
                self._set_links.append(setlink)

        except Exception:
            # Ensure we return collections if something bad happened
            logger.error("Got an exception in runner, returning instances",
                         exc_info=True)

            try:
                await gen.multi([self._pool.release_instances(x)
                                 for x in collections])
            except:
                logger.error("Wat? Got an error returning instances.",
                             exc_info=True)

            # Clear out the setlinks to make sure they aren't cleaned up
            # again
            self._set_links = []

    async def start(self):
        """Fully manage a complete run

        This doesn't return until the run is complete. A reference
        should be held so that the run state can be checked on as
        needed while this is running. This method chains to all the
        individual portions of a run.

        """
        try:
            # Initialize the run
            await self._initialize()

            # Start and manage the run
            await self._run()

            # Terminate the run
            await self._shutdown()
        except:
            await self._cleanup(exc=True)
        else:
            await self._cleanup()

        return True

    async def _initialize(self):
        # Initialize all the collections, this needs to always be done
        # just in case we're recovering
        await self._get_steps()

        # Skip if we're running
        if self.state == RUNNING:
            return

        # Wait for the collections to come up
        self.state_description = "Waiting for running docker instances."
        await gen.multi([setlink.initialize(self.helpers.docker)
                         for setlink in self._set_links])

        self.run.state = RUNNING
        self.run.started_at = datetime.utcnow()
        self._db_session.commit()
        log_threadid("Now running.")

    async def _shutdown(self):
        # If we aren't terminating, we shouldn't have been called
        if self.state != TERMINATING:
            return

        # Tell all the collections to shutdown
        await gen.multi([self._stop_step(s) for s in self._set_links])
        self.run.state = COMPLETED
        self.run.aborted = self.abort
        self._db_session.commit()

    async def _cleanup(self, exc=False):
        if exc:
            # Ensure we try and shut them down
            logger.debug("Exception occurred, ensure containers terminated.",
                         exc_info=True)
            try:
                await gen.multi([self._stop_step(s) for s in self._set_links])
            except Exception:
                logger.error("Le sigh, error shutting down instances.",
                             exc_info=True)

        # Ensure we always release the collections we used
        logger.debug("Returning collections")

        try:
            await gen.multi([self._pool.release_instances(x.ec2_collection)
                             for x in self._set_links])
        except Exception:
            logger.error("Embarassing, error returning instances.",
                         exc_info=True)

        self._set_links = []

    async def _run(self):
        # Skip if we're not running
        if self.state != RUNNING:
            return

        # Main run loop
        while True:
            if self.abort:
                logger.debug("Aborted, exiting run loop.")
                break

            stop = await self._check_steps()
            if stop:
                break

            # Now we sleep for a bit
            await gen.Task(self._loop.add_timeout, time.time() +
                           self.sleep_time)

        # We're done running, time to terminate
        self.run.state = TERMINATING
        self.run.completed_at = datetime.utcnow()
        self._db_session.commit()

    async def _check_steps(self):
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
        dones = await gen.multi([x.is_done(self.helpers.docker)
                                 for x in self._set_links])
        dones = zip(dones, self._set_links)

        # Send shutdown to steps that have completed, we can shut them all
        # down in any order so we run in parallel
        async def shutdown(setlink):
            try:
                await self._stop_step(setlink)
            except:
                logger.error("Exception in shutdown.", exc_info=True)

            setlink.step_record.completed_at = datetime.utcnow()
            self._db_session.commit()
        await gen.multi([shutdown(s) for done, s in dones if done])

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
                await self._start_step(setlink)
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

    async def _start_step(self, setlink):
        setlink.ec2_collection.started = True
        await setlink.start(self.helpers, self._dns_map, self.influxdb_options)

    async def _stop_step(self, setlink):
        # If we're already finished, don't shut things down twice
        if setlink.ec2_collection.finished:
            return
        setlink.ec2_collection.finished = True
        await setlink.stop(self.helpers)

    def _should_start(self, setlink):
        """Given a StepRecordLink, determine if the step should be started."""
        return setlink.step_record.should_start()
