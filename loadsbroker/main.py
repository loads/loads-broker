import os
from functools import partial

import tornado.ioloop

from loadsbroker.db import Database, Run, RUNNING, TERMINATED
from loadsbroker.awsctrl import AWSController
from loadsbroker.dockerctrl import DockerDaemon

# CoreOS-stable-367.1.1
COREOS_IMG = 'ami-3193e801'
USER_DATA = os.path.join(os.path.dirname(__file__), 'aws.yml')
KEY_PATH = '/Users/tarek/.ssh/loads.pem'
USER_NAME = 'core'


# just a prototype. needs tests and reworking
#
def main():
    loop = tornado.ioloop.IOLoop.instance()
    aws = AWSController(io_loop=loop)
    db = Database('sqlite:////tmp/loads.db', echo=True)
    nodes = 15
    ami = COREOS_IMG
    user_data = USER_DATA
    run = Run(ami=ami, nodes=nodes)


    def test(session, instances):
        run.status = RUNNING
        session.commit()

        # do something with the nodes
        for instance in instances:
            loop.add_callback(do_something_with_instance, instance)

        # terminate them
        loop.add_callback(aws.terminate_run, run.uuid)

        # mark the state in the DB

        def set_state(state):
            run.state = state
            session.commit()

        loop.add_callback(set_state, TERMINATED)

        # stop the loop
        loop.add_callback(loop.stop)

    def do_something_with_instance(instance):
        name = instance.tags['Name']
        # let's try to do something with it.
        # first a few checks via ssh
        print 'working with %s' % name
        print aws.run_command(instance, 'ls -lah', KEY_PATH, USER_NAME)

        # port 2375 should be answering something. let's hook
        # it with our DockerDaemon class
        d = DockerDaemon(host='tcp://%s:2375' % instance.public_dns_name)

        # let's list the containers
        print d.get_containers()


    def reserve():
        with db.session() as session:
            session.add(run)

            callback = partial(test, session)

            # reserving 15 boxes - the test function is called when they are ready
            aws.reserve(run.uuid, nodes, ami, user_data=USER_DATA,
                        callback=callback)

    loop.add_callback(reserve)
    loop.start()


if __name__ == '__main__':
    main()
