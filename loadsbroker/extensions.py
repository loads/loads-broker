import os
import time
from io import StringIO
from random import randint
from shlex import quote
from string import Template

import paramiko.client as sshclient
import tornado.ioloop
from tornado import gen
from tornado.httpclient import AsyncHTTPClient

from loadsbroker import logger
from loadsbroker.exceptions import LoadsException
from loadsbroker.dockerctrl import DockerDaemon
from loadsbroker.ssh import makedirs

# Default ping request options.
_PING_DEFAULTS = {
    "method": "HEAD",
    "headers": {"Connection": "close"},
    "follow_redirects": False
}

# The Heka configuration file template. Heka containers on each instance
# forward messages to a central Heka server via TcpOutput.
HEKA_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "hekad.src.toml")

with open(HEKA_CONFIG_PATH, "r") as f:
    HEKA_CONFIG_TEMPLATE = Template(f.read())


class Ping:
    def __init__(self, io_loop=None):
        self._loop = io_loop or tornado.ioloop.IOLoop.instance()
        self._ping_client = AsyncHTTPClient(io_loop=self._loop,
                                            defaults=_PING_DEFAULTS)

    @gen.coroutine
    def ping(self, instance, url, attempts=5, delay=0.5, max_jitter=0.2,
             max_delay=15, **options):
        """Attempts to load a URL to verify its reachable."""
        attempt = 1
        while True:
            try:
                yield self._ping_client.fetch(url, **options)
                return True
            except ConnectionError:
                jitter = randint(0, max_jitter * 100) / 100
                yield gen.Task(self._loop.add_timeout,
                               time.time() + delay + jitter)
                attempt += 1
                delay = min(delay * 2, max_delay)
                if attempt >= attempts:
                    raise


class SSH:
    """SSH client to communicate with instances."""
    def __init__(self, ssh_keyfile):
        self._ssh_keyfile = ssh_keyfile

    def connect(self, instance):
        """Opens an SSH connection to this instance."""
        client = sshclient.SSHClient()
        client.set_missing_host_key_policy(sshclient.AutoAddPolicy())
        client.connect(instance.ip_address, username="core",
                       key_filename=self._ssh_keyfile)
        return client

    def _send_file(self, sftp, local_obj, remote_file):
        # Ensure the base directory for the remote file exists
        base_dir = os.path.dirname(remote_file)
        makedirs(sftp, base_dir)

        # Copy the local file to the remote location.
        sftp.putfo(local_obj, remote_file)

    def upload_file(self, instance, local_obj, remote_file):
        """Upload a file to an instance. Blocks."""
        client = self.connect(instance)
        try:
            sftp = client.open_sftp()
            try:
                self._send_file(sftp, local_obj, remote_file)
            finally:
                sftp.close()
        finally:
            client.close()


class Docker:
    def __init__(self, ssh):
        self.sshclient = ssh

    @gen.coroutine
    def setup_collection(self, collection):
        def setup_docker(ec2_instance):
            instance, state = ec2_instance
            if instance.ip_address is None:
                docker_host = 'tcp://0.0.0.0:7890'
            else:
                docker_host = "tcp://%s:2375" % instance.ip_address

            if not hasattr(state, "docker"):
                state.docker = DockerDaemon(host=docker_host)
        yield collection.map(setup_docker)

    @staticmethod
    def not_responding_instances(collection):
        return [x for x in collection.instances
                if not x.state.docker.responded]

    @gen.coroutine
    def wait(self, collection, interval=5, timeout=600):
        """Waits till docker is available on every instance in the
        collection."""
        end = time.time() + timeout

        not_responded = self.not_responding_instances(collection)

        def get_container(inst):
            try:
                inst.state.docker.get_containers()
                inst.state.docker.responded = True
            except Exception:
                pass

        # Attempt to fetch until they've all responded
        while not_responded and time.time() < end:
            yield [collection.execute(get_container, x) for x in
                   not_responded]

            # Update the not_responded
            not_responded = self.not_responding_instances(collection)

            if not_responded:
                yield collection.wait(interval)

        # Prune the non-responding
        logger.debug("Pruning %d non-responding instances.",
                     len(not_responded))
        collection.remove_instances(not_responded)

    @gen.coroutine
    def is_running(self, collection, container_name):
        """Checks running instances in a collection to see if the provided
        container_name is running on the instance."""
        def has_container(instance):
            all_containers = instance.state.docker.get_containers()
            for _, container in all_containers.items():
                if container_name in container["Image"]:
                    return True
            return False

        results = yield [collection.execute(has_container, x) for x in
                         collection.running_instances()]
        return any(results)

    @gen.coroutine
    def load_containers(self, collection, container_name, container_url):
        """Loads's a container of the provided name to the instance."""
        def load(instance):
            docker = instance.state.docker

            has_container = docker.has_image(container_name)
            if has_container:
                return

            if container_url:
                client = self.sshclient.connect()
                try:
                    output = docker.import_container(client, container_url)
                finally:
                    client.close()
            else:
                output = docker.pull_container(container_name)

            if not docker.has_image(container_name):
                raise LoadsException("Unable to load container: %s", output)
            return output

        yield collection.map(load)

    @gen.coroutine
    def run_containers(self, collection, container_name, env, command_args,
                       volumes={}, ports={}, local_dns=False):
        """Run a container of the provided name with the env/command
        args supplied."""
        if env:
            run_env = env.split("\n")
        else:
            run_env = []

        def run(instance):
            dns = []
            if local_dns:
                dns = [instance.instance.ip_address]
            docker = instance.state.docker
            docker.run_container(container_name, run_env, command_args,
                                 volumes, ports, dns=dns)
        yield collection.map(run)

    @gen.coroutine
    def kill_containers(self, collection, container_name):
        """Kill the container with the provided name."""
        def kill(instance):
            instance.state.docker.kill_container(container_name)
        yield collection.map(kill)

    @gen.coroutine
    def stop_containers(self, collection, container_name, timeout=15):
        """Gracefully stops the container with the provided name and
        timeout."""
        def stop(instance):
            instance.state.docker.stop_container(container_name, timeout)
        yield collection.map(stop)


class CAdvisor:
    def __init__(self, options):
        self.options = options

    @gen.coroutine
    def start(self, collection, docker, ping, database_name):
        options = self.options
        """Launches a cAdvisor container on the instance."""
        volumes = {
            '/': {'bind': '/rootfs', 'ro': True},
            '/var/run': {'bind': '/var/run', 'ro': False},
            '/sys': {'bind': '/sys', 'ro': True},
            '/var/lib/docker': {'bind': '/var/lib/docker', 'ro': True}
        }

        logger.debug("cAdvisor: Writing stats to %s" % database_name)
        command_args = " ".join([
            "-storage_driver=influxdb",
            "-log_dir=/",
            "-storage_driver_db=%s" % quote(database_name),
            "-storage_driver_host=%s:%d" % (quote(options.host),
                                            options.port),
            "-storage_driver_user=%s" % quote(options.user),
            "-storage_driver_password=%s" % quote(options.password),
            "-storage_driver_secure=%d" % options.secure
        ])
        yield docker.run_containers(collection, "google/cadvisor:latest",
                                    None, command_args, volumes,
                                    ports={8080: 8080})

        yield self.wait(collection, ping)

    @gen.coroutine
    def stop(self, collection, docker):
        yield docker.stop_containers(collection, "google/cadvisor:latest")

    @gen.coroutine
    def wait(self, collection, ping):
        def _ping(inst):
            health_url = "http://%s:8080/healthz" % inst.instance.ip_address
            return ping.ping(health_url)
        yield collection.map(_ping)


class Heka:
    def __init__(self, ssh, options):
        self.sshclient = ssh
        self.options = options

    @gen.coroutine
    def start(self, collection, docker, ping):
        """Launches Heka containers on all instances."""

        if not self.options:
            logger.debug("Heka not configured")
            return

        remote_host = self.options.host
        if ":" in remote_host or "%" in remote_host:
            remote_host = "[" + remote_host + "]"

        config_file = HEKA_CONFIG_TEMPLATE.substitute(
            remote_host=remote_host,
            remote_port=self.options.port,
            remote_secure=self.options.secure and "true" or "false")

        volumes = {'/home/core/heka': {'bind': '/heka', 'ro': False}}
        ports = {(8125, "udp"): 8125, 4352: 4352}

        # Upload heka config to all the instances
        def upload_files(inst):
            with StringIO(config_file) as fl:
                self.sshclient.upload_file(inst.instance, fl,
                                           "/home/core/heka/config.toml")
        yield collection.map(upload_files)

        logger.debug("Launching Heka...")
        yield docker.run_containers(collection, "kitcambridge/heka:dev",
                                    None, "hekad -config=/heka/config.toml",
                                    volumes=volumes, ports=ports)

        def ping_heka(inst):
            health_url = "http://%s:4352/" % inst.instance.ip_address
            yield ping.ping(health_url)
        yield collection.map(ping_heka)

    @gen.coroutine
    def stop(self, collection, docker):
        yield docker.stop_containers(collection, "kitcambridge/heka:dev")


class DNSMasq:
    def __init__(self, docker):
        self.docker = docker

    @gen.coroutine
    def start(self, collection, hostmap):
        """Starts dnsmasq on a host with a given host mapping.

        Host mapping is a dict of "Hostname" -> ["IP"].

        """
        records = []
        tmpl = "--host-record={name},{ip}"
        for name, ips in hostmap:
            for ip in ips:
                records.append(tmpl.format(name=name, ip=ip))

        cmd = "dnsmasq " + " ".join(records)
        ports = {(53, "udp"): 53}

        yield self.docker.run_containers(
            collection, "kitcambridge/heka:dev", None, cmd, ports=ports)
