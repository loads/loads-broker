"""AWS Higher Level Abstractions

This module contains higher-level AWS abstractions to make working with AWS
instances and collections of instances easier and less error-prone.

"""
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

from loadsbroker.pooling import thread_pool
from loadsbroker import logger
from loadsbroker.util import retry

def create_key(self, *args):
    key = ':::'.join(str(arg) for arg in args).encode('utf-8')
    return hashlib.md5(key).hexdigest()


@retry(3)
def _reserve(conn, run_id, num, ami, instance_type, user_data, filters,
             reserved_pool, key_pair, security, sqluri, on_node_created):
    # pick some existing instances if they match
    logger.debug('pick some existing instances if they match')
    available = 0
    for reservation in conn.get_all_instances(filters=filters):
        instance = reservation.instances[0]
        if instance.state == 'terminated':
            # old stuff
            continue
        reserved_pool.append(instance)
        available += 1

    # create some if needed
    logger.debug('create some if needed')
    missing = num - available
    futures = []

    if missing > 0:
        image = conn.get_all_images(image_ids=[ami])[0]

        for i in range(missing):
            logger.debug('submitting a thread')
            args = (conn, run_id, num, ami, instance_type, user_data,
                    reserved_pool, key_pair, security, image, sqluri,
                    on_node_created)
            futures.append(thread_pool.submit(_create_instance, *args))

    # Wait for all the threads we submitted to finish
    concurrent.futures.wait(futures)

    # check if we got any exception
    for future in futures:
        exc = future.exception()
        # re-raise the first exception
        if exc is not None:
            raise exc


@retry(3)
def _create_instance(conn, run_id, num, ami, instance_type, user_data,
                     reserved_pool, key_pair, security,
                     image, sqluri, created):
    logger.debug('creating an instance for %s' % run_id)
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

    # XXX should give a timeout here
    while True:
        instance.update()
        if instance.state == 'pending':
            time.sleep(5)
            logger.debug('waiting...')
        else:
            break

    if created is not None:
        created(run_id, name, instance.id, instance.public_dns_name,
                instance.state)

    reserved_pool.append(instance)


class AWSController:

    def __init__(self, security='loads', region='us-west-2',
                 key_pair='loads', io_loop=None, sqluri=None):
        self.conn = connect_to_region(region)
        self.security = security
        self.region = region
        fd, self.host_key_file = tempfile.mkstemp()
        os.close(fd)
        self.key_pair = key_pair
        self.loop = io_loop or tornado.ioloop.IOLoop.instance()
        self.sqluri = sqluri

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
                user_data=None, on_node_created=None):
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
                self.security, self.sqluri, on_node_created)

        # how do I get back future.exception() in this case?
        # this yield does not return a future...
        yield thread_pool.submit(_reserve, *args)

        return reserved_pool

    def run_command(self, instance, command, key_path, user_name):
        """Runs SSH in an instance.
        """
        ssh_client = sshclient_from_instance(instance, key_path,
                                             host_key_file=self.host_key_file,
                                             user_name=user_name)
        return ssh_client.run(command)
