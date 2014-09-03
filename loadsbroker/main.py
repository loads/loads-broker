import os
import sys

import tornado.ioloop

from loadsbroker.awsctrl import AWSController
from loadsbroker.dockerctrl import DockerDaemon

# CoreOS-stable-367.1.1
COREOS_IMG = 'ami-3193e801'
USER_DATA = os.path.join(os.path.dirname(__file__), 'aws.yml')
RUN_ID = 'whadadoo-simple-simple-push'
KEY_PATH = '/Users/tarek/.ssh/loads.pem'
USER_NAME = 'core'



def main():

    def test(instances):
        # do something with the nodes
        for instance in instances:
            loop.add_callback(do_something_with_instance, instance)

        # terminate them
        loop.add_callback(aws.terminate_run, RUN_ID)

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


    loop = tornado.ioloop.IOLoop.instance()
    aws = AWSController(io_loop=loop)

    # reserving 5 boxes
    aws.reserve(RUN_ID, 5, COREOS_IMG, user_data=USER_DATA, callback=test)


    loop.start()


if __name__ == '__main__':
    main()
