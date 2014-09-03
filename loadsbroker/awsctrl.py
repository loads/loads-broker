# XXX todo: mock aws in the tests once it works
#
import random
import time
from threading import Thread
import os
import re
import hashlib
import tempfile

import tornado.ioloop
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
                 key_pair='loads', io_loop=None):
        self.conn = connect_to_region(region)
        self.security = security
        fd, self.host_key_file = tempfile.mkstemp()
        os.close(fd)

        # when we start we want to load all existing instances
        self.instances = {}
        self.sync()
        self.key_pair = key_pair
        self.loop = io_loop or tornado.ioloop.IOLoop.instance()
        cb = tornado.ioloop.PeriodicCallback(self.sync, 5000)
        cb.start()

    def _key(self, *args):
        key = ':::'.join([str(arg) for arg in args])
        return hashlib.md5(key).hexdigest()

    def reserve(self, run_id, num, ami, instance_type='t1.micro',
                user_data=None, callback=None):
        if user_data is not None and os.path.exists(user_data):
            with open(user_data) as f:
                user_data = f.read()

        # building a unique key for the run
        key = self._key(run_id, ami, instance_type, user_data)

        # let's get some!
        #
        # XXX todo filter out busy boxes
        filters = {"tag:Project": "loads", "tag:Key": key,
                   "tag:RunId": run_id}

        match = []
        for reservation in self.conn.get_all_instances(filters=filters):
            instance = reservation.instances[0]
            if instance.state == 'terminated':
                # old stuff
                continue
            match.append(instance)

        # create some if needed
        missing = num - len(match)
        if missing > 0:
            for i in range(missing):
                name, instance = self.create_server(run_id, ami,
                                                    instance_type=instance_type,
                                                    user_data=user_data)
                match.append(instance)

        # happy us
        ready = []
        reserved = match[:num]
        for instance in reserved:
            self.loop.add_callback(self.check_instance, instance, ready,
                                   num, callback)

    def check_instance(self, instance, ready, num, callback):
        print 'Checking on %s' % instance
        instance.update()
        print 'State is %s' % instance.state
        if instance.state not in ('pending', ):
            print '%s ready' % instance.id
            ready.append(instance)
            if len(ready) == num:
                callback(ready)
        else:
            self.loop.call_later(10, self.check_instance, instance,
                                    ready, num, callback)


    def get_instances(self, **tags):
        filters = {"tag:Project": "loads"}
        for tag, value in tags.items():
            filters['tag:%s' % tag] = value

        return [reservation.instances[0] for reservation in
                self.conn.get_all_instances(filters=filters)]

    def run_command(self, instance, command, key_path, user_name):
        ssh_client = sshclient_from_instance(instance, key_path,
                                             host_key_file=self.host_key_file,
                                             user_name=user_name)
        return ssh_client.run(command)

    def create_server(self, run_id, ami, instance_type='t1.micro', user_data=None):
        if user_data is not None and os.path.exists(user_data):
            with open(user_data) as f:
                user_data = f.read()

        key = self._key(run_id, ami, instance_type, user_data)
        name = 'loads-%d' % random.randint(1, 9999)
        while name in self.instances:
            name = 'loads-%d' % random.randint(1, 9999)

        image = self.conn.get_all_images(image_ids=[ami])[0]
        reservation = image.run(1, 1, key_name=self.key_pair,
                                security_groups=[self.security],
                                instance_type=instance_type,
                                user_data=user_data)
        instance = reservation.instances[0]
        self.conn.create_tags([instance.id], {"Name": name})
        self.conn.create_tags([instance.id], {"Project": "loads"})
        self.conn.create_tags([instance.id], {"Key": key})
        self.conn.create_tags([instance.id], {"RunId": run_id})
        self.instances[name] = instance
        return name, instance

    def sync(self):
        # refreshing our internal list
        for instance in self.get_instances():
            name = instance.tags['Name']
            if name not in self.instances:
                self.instances[name] = instance

        # for each instance, updating its state
        for instance in self.instances.values():
            instance.update()

    def terminate_run(self, run_id):
        print 'terminate run!'
        for instance in self.get_instances(RunId=run_id):
            self.conn.terminate_instances(instance_ids=[instance.id])
            del self.instances[instance.tags['Name']]

    def terminate_server(self, name):
        # XXX if not found in memory we need to find it back through
        # the Name tag - so the broker support restarts
        id = self.instances[name].id
        self.conn.terminate_instances(instance_ids=[id])
        del self.instances[name]
