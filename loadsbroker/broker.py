import os
from functools import partial
from uuid import uuid4

from sqlalchemy.orm.exc import NoResultFound
from tornado import gen

from loadsbroker import logger, aws
from loadsbroker.api import _DEFAULTS
from loadsbroker.db import Database, Run, Node, RUNNING, TERMINATED


class Broker:
    def __init__(self, io_loop, sqluri, ssh_key, ssh_username, aws_port=None):
        self.loop = io_loop
        user_data = _DEFAULTS["user_data"]
        if user_data is not None and os.path.exists(user_data):
            with open(user_data) as f:
                user_data = f.read()

        self.pool = aws.EC2Pool("1234", user_data=user_data,
                                io_loop=self.loop, port=aws_port)
        self.db = Database(sqluri, echo=True)
        self.sqluri = sqluri
        self.ssh_key = ssh_key
        self.ssh_username = ssh_username

    def get_runs(self):
        # XXX filters, batching
        runs = self.db.session().query(Run).all()
        return [run.json() for run in runs]

    @gen.coroutine
    def _test(self, run, session, collection):
        run.status = RUNNING
        session.commit()

        # Create all the nodes in the db
        for inst in collection._instances:
            i = inst._instance
            self._node_created(session, run.uuid, i.id,
                               i.public_dns_name, i.state)

        # Wait for all the instances to come up
        yield collection.wait_for_docker()
        logger.debug("Finished waiting for docker on all instances")

        # return the instances to the pool
        yield self.pool.return_instances(collection)

        # reap the pool
        logger.debug("Reaping instances...")
        yield self.pool.reap_instances()
        logger.debug("Finished terminating.")

        # mark the state in the DB
        run.state = TERMINATED
        session.commit()
        logger.debug("Finished test run, all cleaned up.")

    @gen.coroutine
    def _run_instance(self, instance):
        name = instance.tags['Name']
        # let's try to do something with it.
        # first a few checks via ssh
        logger.debug('working with %s' % name)
        # logger.debug(self.aws.run_command(instance, 'ls -lah', self.ssh_key,
        #             self.ssh_username))

        # port 2375 should be answering something. let's hook
        # it with our DockerDaemon class
        # d = DockerDaemon(host='tcp://%s:2375' % instance.public_dns_name)

        # let's list the containers
        # logger.debug(d.get_containers())

    def _node_created(self, session, run_id, aws_id, aws_public_dns,
                      aws_state):
        try:
            run = session.query(Run).filter(Run.uuid == run_id).one()
        except NoResultFound:
            # well..
            # XXX
            logger.debug('Run not found in DB')
        else:
            logger.debug('found the run in the db: %s' % str(run))
            logger.debug('instance id is : %s' % str(aws_id))

            name = "loads-" + str(uuid4())
            node = Node(name=name, aws_id=aws_id,
                        aws_public_dns=aws_public_dns,
                        aws_state=aws_state,
                        run_id=run.id)

            session.add(node)
            session.commit()
            logger.debug('Added a Node in the DB for this run')

    def run_test(self, **options):
        nodes = options.pop('nodes')
        options.pop("user_data")

        run = Run(**options)
        session = self.db.session()
        session.add(run)
        session.commit()

        callback = partial(self._test, run, session)
        logger.debug("requesting instances")
        self.pool.request_instances(run.uuid, count=int(nodes),
                                    inst_type="t1.micro", callback=callback)

        return run.uuid
