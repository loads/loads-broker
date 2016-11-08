""" Interacts with a Docker Daemon on a remote instance"""
import random

import docker
from requests.exceptions import ConnectionError, Timeout

from loadsbroker.util import retry

DOCKER_RETRY_EXC = (ConnectionError, Timeout)


def split_container_name(container_name):
    """Pulls apart a container name from its tag"""
    parts = container_name.split(":")
    if len(parts) > 1:
        return parts
    else:
        return parts, None


class DockerDaemon:

    def __init__(self, host, timeout=5):
        self.host = host
        self.timeout = timeout
        self.responded = False
        self._client = docker.Client(base_url=host, timeout=timeout)

    def get_containers(self, all=False):
        """Returns a list of containers

        :param all: Whether to include **non-running** containers.

        """
        return {cont['Id']: cont
                for cont in self._client.containers(all=all)}

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
        self._client.remove_container(cid, force=True)

    def stop(self, cid, timeout=15):
        """Stops and removes a container."""
        self._client.stop(cid, timeout)
        self._client.wait(cid)
        self._client.remove_container(cid)

    def pull_container(self, container_name):
        """Pulls a container image from the repo/tag for the provided
        container name"""
        result = self._client.pull(container_name, stream=True)
        return list(result)

    def import_container(self, client, container_url):
        """Imports a container from a URL"""
        stdin, stdout, stderr = client.exec_command(
            'curl %s | docker load' % container_url)
        # Wait for termination
        output = stdout.channel.recv(4096)
        stdin.close()
        stdout.close()
        stderr.close()
        return output

    @retry(on_exception=lambda exc: isinstance(exc, DOCKER_RETRY_EXC))
    def has_image(self, container_name):
        """Indicates whether this instance already has the desired
        container name/tag loaded.

        Example of what the images command output looks like:

            [{'Created': 1406605442,
              'RepoTags': ['bbangert/simpletest:dev'],
              'Id': '824823...31ae0d6fc69e6e666a4b44118b0a3',
              'ParentId': 'da7b...ee6b9eb2ee47c2b1427eceb51d291a',
              'Size': 0,
              'VirtualSize': 1400958681}]

        """
        name, tag = split_container_name(container_name)
        images = self._client.images(all=True)
        return any(container_name in image["RepoTags"] for image in images)

    def run_container(self, container_name, env, command_args, volumes={},
                      ports={}, dns=[], pid_mode=None):
        """Run a container given the container name, env, command args, data
        volumes, and port bindings."""

        expose = []
        port_bindings = {}
        for port in ports.keys():
            if isinstance(port, tuple):
                proto = port[1] if len(port) == 2 else "tcp"
                key = "%d/%s" % (port[0], proto)
            else:
                key = port
            port_bindings[key] = ports[port]
            expose.append(port)

        result = self._client.create_container(
            container_name, command=command_args, environment=env,
            volumes=[volume['bind'] for volume in volumes.values()],
            ports=expose)

        container = result["Id"]
        result = self._client.start(container, binds=volumes,
                                    port_bindings=port_bindings, dns=dns,
                                    pid_mode=pid_mode)
        response = self._client.inspect_container(container)
        return response

    def containers_by_name(self, container_name):
        """Yields all containers that match the given name."""
        containers = self._client.containers()
        return (container for container in containers
                if container_name in container["Image"])

    def kill_container(self, container_name):
        """Locate the container of the given container_name and kill
        it"""
        for container in self.containers_by_name(container_name):
            self.kill(container["Id"])

    def stop_container(self, container_name, timeout=15):
        """Locates and gracefully stops a container by name."""
        for container in self.containers_by_name(container_name):
            self.stop(container["Id"], timeout)
