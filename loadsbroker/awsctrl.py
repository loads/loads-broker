import random
import time
from threading import Thread

import boto
from boto.ec2 import connect_to_region


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

    def create_server(self, ami, instance_type='t1.micro'):
        name = 'loads-%d' % random.randint(1, 9999)
        image = self.conn.get_all_images(image_ids=[ami])[0]
        reservation = image.run(1, 1, key_name=self.key_pair,
                                security_groups=[self.security],
                                instance_type=instance_type)
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


if __name__ == '__main__':
    aws = AWSController()
    name, instance = aws.create_server('ami-01108231')
    print 'Created %s - %s' % (name, id)

    # now let's wait until it's ready
    while instance.state == 'pending':
        instance.update()
        print instance.state
        time.sleep(10)

    print('We got a box, plublic dns is %r' % instance.public_dns_name)

    # let's kill it
    print 'terminating it'
    aws.terminate_server(name)

