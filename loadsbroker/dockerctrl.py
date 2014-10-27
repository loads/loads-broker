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
        if host == 'tcp://46.51.219.63:2375':
            # XXX hardcoded detection of Moto
            # we want to force in that case our local fake docker daemon
            # until Moto let us configure that
            # see https://github.com/spulec/moto/issues/212
            host = 'tcp://127.0.0.1:7890'

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
        for image in images:
            if container_name in image["RepoTags"]:
                return True
        return False

    def run_container(self, container_name, env, command_args, volumes={},
                      ports={}):
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
        return self._client.start(container, binds=volumes,
                                  port_bindings=port_bindings)

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
