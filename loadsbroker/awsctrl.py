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
    key = ':::'.join([str(arg) for arg in args])
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
    print ('create some if needed')
    missing = num - available
    threads = []

    if missing > 0:
        image = conn.get_all_images(image_ids=[ami])[0]

        for i in range(missing):
            print 'started a thread'
            args = (conn, run_id, num, ami, instance_type, user_data,
                    reserved_pool, key_pair, security, image)
            th = Thread(target=_create_instance, args=args)
            threads.append(th)
            th.start()

    # waiting here for all threads to finish (=pool full)
    while len(reserved_pool) < num:
        time.sleep(1.)

    # terminating threads
    for th in threads:
        th.join()


def _create_instance(conn, run_id, num, ami, instance_type, user_data,
                     reserved_pool, key_pair, security,
                     image):
    print 'creating an instance for %s' % run_id
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

    def _check_pool(self, num, pool, thread, callback):
        print 'check %d/%d' % (len(pool), num)
        if len(pool) < num:
            self.loop.call_later(10, self._check_pool, num, pool, thread,
                                 callback)
            return

        # we got all our boxes, let's clean the thread
        thread.join()
        callback(pool)

    #
    # Public API
    #
    def terminate_run(self, run_id):
        """ Terminates all instances associated to a run_id
        """
        for instance in self.get_instances(RunId=run_id):
            self.conn.terminate_instances(instance_ids=[instance.id])

    def get_instances(self, **tags):
        """Returns a list of instances, matching the provided tags values.
        """
        filters = {"tag:Project": "loads"}
        for tag, value in tags.items():
            filters['tag:%s' % tag] = value

        return [reservation.instances[0] for reservation in
                self.conn.get_all_instances(filters=filters)]

    def reserve(self, run_id, num, ami, instance_type='t1.micro',
                user_data=None, callback=None):
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

        thread = Thread(target=_reserve, args=args)
        thread.start()

        # now just check periodically if we're ready to go
        self.loop.add_callback(self._check_pool, num, reserved_pool, thread,
                               callback)

    def run_command(self, instance, command, key_path, user_name):
        """Runs SSH in an instance.
        """
        ssh_client = sshclient_from_instance(instance, key_path,
                                             host_key_file=self.host_key_file,
                                             user_name=user_name)
        return ssh_client.run(command)
