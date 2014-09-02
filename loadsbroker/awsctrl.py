# XXX todo: mock aws in the tests once it works
#
import random
import time
from threading import Thread
import os
import re

import boto
from boto.ec2 import connect_to_region
from boto.manage.cmdshell import sshclient_from_instance
import pyaml

# create a ~/.boto file with
#
# [Credentials]
# aws_access_key_id = YOURACCESSKEY
# aws_secret_access_key = YOURSECRETKEY

class AWSController(object):

    def __init__(self, security='loads', region='us-west-2',
                 key_pair='loads'):
        self.conn = connect_to_region(region)
        self.security = security
        self.instances = {}
        self.key_pair = key_pair

    def run_command(self, instance, command, key_path, user_name):
        ssh_client = sshclient_from_instance(instance, key_path,
                                             user_name=user_name)
        return ssh_client.run(command)


    def create_server(self, ami, instance_type='t1.micro', user_data=None):
        if user_data is not None and os.path.exists(user_data):
            with open(user_data) as f:
                user_data = f.read()

        name = 'loads-%d' % random.randint(1, 9999)
        image = self.conn.get_all_images(image_ids=[ami])[0]
        reservation = image.run(1, 1, key_name=self.key_pair,
                                security_groups=[self.security],
                                instance_type=instance_type,
                                user_data=user_data)
        instance = reservation.instances[0]
        self.conn.create_tags([instance.id], {"Name": name})
        self.instances[name] = instance
        return name, instance

    def update(self):
        for instance in self.instances.items():
            instance.update()

    def terminate_server(self, name):
        # XXX if not found in memory we need to find it back through
        # the Name tag - so the broker support restarts
        id = self.instances[name].id
        self.conn.terminate_instances(instance_ids=[id])
        del self.instances[name]




# CoreOS-stable-367.1.1
COREOS_IMG = 'ami-3193e801'
USER_DATA = os.path.join(os.path.dirname(__file__), 'aws.yml')


if __name__ == '__main__':

    from loadsbroker.dockerctrl import DockerDaemon
    aws = AWSController()
    name, instance = aws.create_server(COREOS_IMG, user_data=USER_DATA)
    print 'Created %s - %s' % (name, instance.id)

    # now let's wait until it's ready
    while instance.state == 'pending':
        instance.update()
        print instance.state
        time.sleep(10)

    print('We got a box, plublic dns is %r' % instance.public_dns_name)

    key_path = '/Users/tarek/.ssh/loads.pem'
    user_name = 'core'

    try:
        # let's try to do something with it.
        # first a few checks via ssh

        print aws.run_command(instance, 'ls -lah', key_path, user_name)


        # port 2375 should be answering something. let's hook
        # it with our DockerDaemon class

        d = DockerDaemon(host='tcp://%s:2375' % instance.public_dns_name)

        # let's list the containers
        print d.get_containers()

    finally:
        # let's kill it
        print 'terminating it'
        aws.terminate_server(name)

