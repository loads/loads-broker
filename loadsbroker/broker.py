from functools import partial

from tornado import gen

from loadsbroker.db import Database, Run, RUNNING, TERMINATED
from loadsbroker.awsctrl import AWSController
from loadsbroker.dockerctrl import DockerDaemon
from loadsbroker import logger


class Broker(object):
    def __init__(self, io_loop, sqluri, ssh_key, ssh_username):
        self.loop = io_loop
        self.aws = AWSController(io_loop=self.loop, sqluri=sqluri)
        self.db = Database(sqluri, echo=True)
        self.ssh_key = ssh_key
        self.ssh_username = ssh_username

    def get_runs(self):
        # XXX filters, batching
        runs = self.db.session().query(Run).all()
        return [run.json() for run in runs]

    @gen.coroutine
    def _test(self, run, session, instances):
        run.status = RUNNING
        session.commit()

        # do something with the nodes, catch exceptions
        try:
            yield [self._run_instance(inst) for inst in instances]
        except:
            logger.debug("Error running commands, moving on.")

        # terminate them
        yield self.aws.terminate_run(run.uuid)
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
        logger.debug(self.aws.run_command(instance, 'ls -lah', self.ssh_key,
                     self.ssh_username))

        # port 2375 should be answering something. let's hook
        # it with our DockerDaemon class
        d = DockerDaemon(host='tcp://%s:2375' % instance.public_dns_name)

        # let's list the containers
        logger.debug(d.get_containers())

    def run_test(self, **options):
        user_data = options.pop('user_data')
        nodes = options.pop('nodes')

        run = Run(**options)
        session = self.db.session()
        session.add(run)
        session.commit()

        callback = partial(self._test, run, session)
        self.aws.reserve(run.uuid, nodes, run.ami,
                         user_data=user_data,
                         callback=callback)

        return run.uuid