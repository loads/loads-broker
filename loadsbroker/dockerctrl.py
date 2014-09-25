""" Interacts with a Docker Dameon
"""
import os
import random
import sys

import docker


def split_container_name(container_name):
    parts = container_name.split(":")
    if len(parts) > 1:
        return parts
    else:
        return parts, None


class DockerDaemon:

    def __init__(self, host=None, timeout=5):
        if host is None:
            try:
                host = os.environ['DOCKER_HOST']
            except KeyError:
                raise ValueError('No host defined and DOCKER_HOST not set'
                                 ' in env')
        self.host = host
        self.timeout = timeout
        self._client = docker.Client(base_url=host, timeout=timeout)

    def get_containers(self, all=False):
        """Returns a list of containers

        :param all: Whether to include **non-running** containers.

        """
        containers = self._client.containers(all=all)
        res = {}
        for container in containers:
            res[container['Id']] = container
        return res

    def _create_container(self, image, cmd=None):
        """creates a container
        """
        name = 'loads_%d' % random.randint(1, 9999)
        container = self._client.create_container(image, name=name,
                                                  command=cmd,
                                                  detach=True)
        id = container['Id']
        self._client.start(container=id, publish_all_ports=True)
        return name, id

    def run(self, commands, image):
        """Runs commands in a new container.

        Sends back a blocking iterator on the log output.
        """
        cmd = '/bin/sh -c "%s"' % ';'.join(commands)
        cname, cid = self._create_container(image, cmd=cmd)
        return cid, self._client.attach(cid, stream=True, logs=True)

    def kill(self, cid):
        """Kills and remove a container.
        """
        self._client.kill(cid)
        self._client.remove_container(cid)

    def pull_container(self, container_name):
        """Pulls a container image from the repo/tag for the provided
        container name"""
        name, tag = split_container_name(container_name)
        return self._client.pull(name, tag=tag, stream=True)

    def run_container(self, container_name, env, command_args):
        """Run a container given the container name, env, and command
        args"""
        result = self._client.create_container(
            container_name, command=command_args, environment=env)
        container = result["Id"]
        return self._client.start(container)

    def kill_container(self, container_name):
        """Locate the container of the given container_name and kill
        it"""
        containers = self._client.containers()
        for container in containers:
            if container_name not in container["Image"]:
                continue

            self.kill(container["Id"])


if __name__ == '__main__':
    # export DOCKER_HOST=tcp://192.168.59.103:2375
    daemon = DockerDaemon()
    cid, r = daemon.run(['date', 'sleep 2', 'ls'], 'ubuntu')
    try:
        print('Running on %r' % cid)
        for output in r:
            sys.stdout.write(output)
        print
    finally:
        daemon.kill(cid)
