# XXX todo: mock aws in the tests once it works
#
import concurrent.futures
import os
import hashlib
import tempfile
import time
from uuid import uuid4

import tornado.ioloop
from boto.ec2 import connect_to_region
from boto.manage.cmdshell import sshclient_from_instance
from tornado import gen

from .pooling import thread_pool

# create a ~/.boto file with
#
# [Credentials]
# aws_access_key_id = YOURACCESSKEY
# aws_secret_access_key = YOURSECRETKEY


def create_key(self, *args):
    key = ':::'.join(str(arg) for arg in args).encode('utf-8')
    return hashlib.md5(key).hexdigest()


def _reserve(conn, run_id, num, ami, instance_type, user_data, filters,
             reserved_pool, key_pair, security):
    # pick some existing instances if they match
    print('pick some existing instances if they match')
    available = 0
    for reservation in conn.get_all_instances(filters=filters):
        instance = reservation.instances[0]
        if instance.state == 'terminated':
            # old stuff
            continue
        reserved_pool.append(instance)
        available += 1

    # create some if needed
    print('create some if needed')
    missing = num - available
    futures = []

    if missing > 0:
        image = conn.get_all_images(image_ids=[ami])[0]

        for i in range(missing):
            print('submitting a thread')
            args = (conn, run_id, num, ami, instance_type, user_data,
                    reserved_pool, key_pair, security, image)
            futures.append(thread_pool.submit(_create_instance, *args))

    # Wait for all the threads we submitted to finish
    concurrent.futures.wait(futures)


def _create_instance(conn, run_id, num, ami, instance_type, user_data,
                     reserved_pool, key_pair, security,
                     image):
    print('creating an instance for %s' % run_id)
    if user_data is not None and os.path.exists(user_data):
        with open(user_data) as f:
            user_data = f.read()

    key = create_key(run_id, ami, instance_type, user_data)
    name = 'loads-%s' % str(uuid4())
    reservation = image.run(1, 1, key_name=key_pair,
                            security_groups=[security],
                            instance_type=instance_type,
                            user_data=user_data)
    instance = reservation.instances[0]
    conn.create_tags([instance.id], {"Name": name})
    conn.create_tags([instance.id], {"Project": "loads"})
    conn.create_tags([instance.id], {"Key": key})
    conn.create_tags([instance.id], {"RunId": run_id})

    while instance.state == 'pending':
        instance.update()
        time.sleep(5)

    reserved_pool.append(instance)


class AWSController(object):

    def __init__(self, security='loads', region='us-west-2',
                 key_pair='loads', io_loop=None):
        self.conn = connect_to_region(region)
        self.security = security
        self.region = region
        fd, self.host_key_file = tempfile.mkstemp()
        os.close(fd)
        self.key_pair = key_pair
        self.loop = io_loop or tornado.ioloop.IOLoop.instance()

    #
    # Public API
    #
    @gen.coroutine
    def terminate_run(self, run_id):
        """ Terminates all instances associated to a run_id
        """
        instances = self.get_instances(RunId=run_id)
        return self.conn.terminate_instances([inst.id for inst in instances])

    def get_instances(self, **tags):
        """Returns a list of instances, matching the provided tags values.
        """
        filters = {"tag:Project": "loads"}
        for tag, value in tags.items():
            filters['tag:%s' % tag] = value

        instances = self.conn.get_all_instances(filters=filters)
        return [reservation.instances[0] for reservation in instances]

    @gen.coroutine
    def reserve(self, run_id, num, ami, instance_type='t1.micro',
                user_data=None):
        """Reserve instances for a run. Try to reuse existing instances.
        """
        if user_data is not None and os.path.exists(user_data):
            with open(user_data) as f:
                user_data = f.read()

        # building a unique key for the run
        key = create_key(run_id, ami, instance_type, user_data)

        # let's get some boxes!
        # XXX todo filter out busy boxes
        filters = {"tag:Project": "loads", "tag:Key": key,
                   "tag:RunId": run_id}

        reserved_pool = []

        args = (self.conn, run_id, num, ami, instance_type,
                user_data, filters, reserved_pool, self.key_pair,
                self.security)
        yield thread_pool.submit(_reserve, *args)

        return reserved_pool

    def run_command(self, instance, command, key_path, user_name):
        """Runs SSH in an instance.
        """
        ssh_client = sshclient_from_instance(instance, key_path,
                                             host_key_file=self.host_key_file,
                                             user_name=user_name)
        return ssh_client.run(command)
