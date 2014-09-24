"""Broker Orchestration

The Broker is responsible for:

* Coordinating runs
* Ensuring run transitions
* Providing a rudimentary public API for use by the CLI/Web clients

"""
import os
import time
from datetime import datetime, timedelta
from functools import partial
from uuid import uuid4

from tornado import gen

from loadsbroker import logger, aws
from loadsbroker.api import _DEFAULTS
from loadsbroker.db import (
    Collection,
    Database,
    Run,
    Strategy,
    INITIALIZING,
    RUNNING,
    TERMINATING,
    COMPLETED
)
from loadsbroker.exceptions import LoadsException


class Broker:
    def __init__(self, io_loop, sqluri, ssh_key, ssh_username, aws_port=None,
                 aws_owner_id="595879546273", aws_use_filters=True):
        self.loop = io_loop
        user_data = _DEFAULTS["user_data"]
        if user_data is not None and os.path.exists(user_data):
            with open(user_data) as f:
                user_data = f.read()

        self.pool = aws.EC2Pool("1234", user_data=user_data,
                                io_loop=self.loop, port=aws_port,
                                owner_id=aws_owner_id,
                                use_filters=aws_use_filters)

        self.db = Database(sqluri, echo=True)
        self.sqluri = sqluri
        self.ssh_key = ssh_key
        self.ssh_username = ssh_username

        # Run managers keyed by uuid
        self._runs = {}

    def get_runs(self):
        # XXX filters, batching
        runs = self.db.session().query(Run).all()
        return [run.json() for run in runs]

    @gen.coroutine
    def _test(self, run, session, collection):
        run.status = RUNNING
        session.commit()

        # Wait for all the instances to come up
        yield collection.wait_for_docker()
        logger.debug("Finished waiting for docker on all instances")

        # XXX I guess we should return here and let the test happen?
        # looks like we're reaping the instance right away

        # return the instances to the pool
        yield self.pool.return_instances(collection)

        # reap the pool
        logger.debug("Reaping instances...")
        yield self.pool.reap_instances()
        logger.debug("Finished terminating.")

        # mark the state in the DB
        run.state = COMPLETED
        session.commit()
        logger.debug("Finished test run, all cleaned up.")

    def run_test(self, **options):
        nodes = options.pop('nodes')
        options.pop("user_data")

        run = Run(**options)
        session = self.db.session()
        session.add(run)
        session.commit()

        callback = partial(self._test, run, session)
        logger.debug("requesting instances")
        collection_uuid = str(uuid4())

        self.pool.request_instances(
            run.uuid, collection_uuid, count=int(nodes),
            inst_type="t1.micro", callback=callback)

        return run.uuid


class RunManager:
    """Manages the life-cycle of a load run.

    """
    def __init__(self, db_session, pool, io_loop, run):
        self.run = run
        self._db_session = db_session
        self._pool = pool
        self._loop = io_loop
        self._collections = []
        self._collection_pairs = []
        self.abort = False

    @classmethod
    def new_run(cls, db_session, pool, io_loop, strategy_name):
        """Create a new run manager for the given strategy name

        This creates a new run for this strategy and initializes it.

        :param db_session: SQLAlchemy database session
        :param pool: AWS EC2Pool instance to allocate from
        :param io_loop: A tornado io loop
        :param strategy_name: The strategy name to use for this run

        :returns: New RunManager in the process of being initialized,
                  along with a future tracking the run.

        """
        strategy = Strategy.load_with_collections(db_session, strategy_name)
        if not strategy:
            raise LoadsException("Unable to find strategy: %s", strategy_name)

        # Create the run for this manager
        run = Run()
        run.strategy = strategy
        db_session.add(run)
        db_session.commit(run)
        run_manager = cls(db_session, pool, io_loop, run)
        future = run_manager.run()
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
    def _get_collections(self):
        """Request all the collection instances needed from the pool

        This is a separate method as both the recover run and new run
        will need to run this identically.

        """
        self._collections = yield [
            self._pool.request_instances(run.uuid, c.uuid,
                count=c.instance_count,
                inst_type=c.instance_type,
                region=c.instance_region)
            for c in self.run.strategy.collections]

        # Setup the collection pairs
        coll_by_uuid = {x.uuid: x for x in run.strategy.collections}
        for coll in self._collections:
            self._collection_pairs.append(coll, coll_by_uuid[coll.uuid])

    @gen.coroutine
    def run(self):
        """Fully manage a complete run

        This doesn't return until the run is complete. A reference
        should be held so that the run state can be checked on as
        needed while this is running. This method chains to all the
        individual portions of a run.

        """
        # Initialize the run
        yield self._initialize()

        # Start and manage the run
        yield self._run()

        # Terminate the run
        yield self._shutdown()

        return True

    @gen.coroutine
    def _initialize(self):
        # Skip if we're running
        if self.state == RUNNING:
            return

        # Initialize all the collections
        yield self._get_collections

        # Wait for docker on all the collections to come up
        yield [x.wait_for_docker() for x in self._collections]

        # Pull the appropriate container for every collection
        yield [coll.pull_container(info.container_name)
            for coll, info in self._collection_pairs
        ]

        self.run.state = RUNNING
        self.run.started_at = datetime.utcnow()
        self._db_session.commit()

    @gen.coroutine
    def _run(self):
        # Skip if we're not running
        if self.state != RUNNING:
            return

        # We're not done until every collection has been run, and is now
        # done
        while True:
            if self.abort:
                break

            all_started = all([x.started for x in self._collections])

            if all_started:
                # Check to see if they're all done, if so we can break
                # the loop
                dones = yield [
                    self.collection_is_done(coll, info)
                    for coll, info in self._collection_pairs]
                if all(dones):
                    break

            # Not every collection has been started, check to see which
            # ones should be started and start them, then sleep
            starts = yield [self.collection_should_start(coll, info)
                for coll, info in self._collection_pairs]

            for start, pair in zip(starts, self._collection_pairs):
                if not start:
                    continue
                coll, info = pair

                # XXX This returns a future which we ignore, at the very
                # least it should be logged if this collection fails to
                # start, or perhaps an immediate full abort of the test run
                # We ignore the future rather than waiting on it so we can
                # continue starting more collections if need be.
                coll.start_container(info.container_name,
                                     env=info.environment_data,
                                     args=info.additional_command_args)

            # Now we sleep for one minute
            # XXX This may need to be configurable
            yield gen.Task(self._loop.add_timeout, time.time() + 60)

        # We're done running, time to terminate
        self.run.state = TERMINATING
        self.run.completed_at = datetime.utcnow()
        self._db_session.commit()

    @gen.coroutine
    def _shutdown(self):
        # If we aren't terminating, we shouldn't have been called
        if self.state != TERMINATING:
            return

        # Tell all the collections to shutdown
        yield [coll.shutdown_containers() for coll in self._collections]

        self.run.state = COMPLETED
        self._db_session.commit()

    @gen.coroutine
    def collection_is_done(self, collection, info):
        """Given an EC2 collection and its collection info, determine
        if the collection has finished or should be terminated."""
        # If the container is no longer running in the collection, its done
        running = yield collection.container_is_running(info.container_name)
        if not any(running):
            # They've all stopped, this collection is done.
            return True

        # If we've been running past the started_at + run_max_time then this
        # should be shut-down
        now = datetime.utcnow()
        if info.started_at + timedelta(seconds=info.run_max_time) > now:
            return True
        else:
            # This collection isn't done
            return False

    @gen.coroutine
    def collection_should_start(self, collection, info):
        """Given an EC2 collection and its collection info, determine
        if the collection should be started."""
        # If the collection is already running, this is a moot point since
        # we can't start it again
        if collection.started:
            return False

        # If we've waited longer than the delay
        now = datetime.utcnow()
        if now >= self.run.started_at + timedelta(seconds=info.run_delay):
            return True
        else:
            return False
