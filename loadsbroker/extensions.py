"""Loads run-time extensions

These loads components are considered extensions as they extend the underlying
AWS instances to add feature support and state maintenance. This composition
avoids excessively large AWS instance classes as external objects can augment
the AWS instances as needed to retain their information.

"""
import os
import time
from io import StringIO
from random import randint
from string import Template
from typing import Dict, Optional
from collections import namedtuple

import paramiko.client as sshclient
import tornado.ioloop
from tornado import gen
from tornado.httpclient import AsyncHTTPClient

from loadsbroker import logger
from loadsbroker.aws import EC2Collection
from loadsbroker.dockerctrl import DOCKER_RETRY_EXC, DockerDaemon
from loadsbroker.ssh import makedirs
from loadsbroker.util import join_host_port, retry

# Default ping request options.
_PING_DEFAULTS = {
    "method": "HEAD",
    "headers": {"Connection": "close"},
    "follow_redirects": False
}

# The Heka configuration file template. Heka containers on each instance
# forward messages to a central Heka server via TcpOutput.
HEKA_CONFIG_PATH = os.path.join(os.path.dirname(__file__), "heka",
                                "config.src.toml")

with open(HEKA_CONFIG_PATH, "r") as f:
    HEKA_CONFIG_TEMPLATE = Template(f.read())


HEKA_NOINFLUX_PATH = os.path.join(os.path.dirname(__file__), "heka",
                                  "config_no_influx.src.toml")

with open(HEKA_NOINFLUX_PATH, "r") as f:
    HEKA_NOINFLUX_TEMPLATE = Template(f.read())


class Ping:
    """Basic ping extension that fetches a HTTP URL to verify it
    can be loaded."""
    def __init__(self, io_loop=None):
        self._loop = io_loop or tornado.ioloop.IOLoop.instance()
        self._ping_client = AsyncHTTPClient(io_loop=self._loop,
                                            defaults=_PING_DEFAULTS)

    async def ping(self,
                   url,
                   attempts=5,
                   delay=0.5,
                   max_jitter=0.2,
                   max_delay=15,
                   **options):
        """Attempts to load a URL to verify its reachable."""
        attempt = 1
        while True:
            try:
                await self._ping_client.fetch(url, **options)
                return True
            except ConnectionError:
                jitter = randint(0, max_jitter * 100) / 100
                await gen.Task(self._loop.add_timeout,
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

    async def reload_sysctl(self, collection):
        def _reload(inst):
            client = self.connect(inst.instance)
            try:
                stdin, stdout, stderr = client.exec_command(
                    "sudo sysctl -p /etc/sysctl.conf")
                output = stdout.channel.recv(4096)
                stdin.close()
                stdout.close()
                stderr.close()
                return output
            finally:
                client.close()
        await collection.map(_reload)


class Docker:
    """Docker commands for AWS instances using :class:`DockerDaemon`"""
    def __init__(self, ssh):
        self.sshclient = ssh

    async def setup_collection(self, collection):
        def setup_docker(ec2_instance):
            instance, state = ec2_instance
            if instance.ip_address is None:
                docker_host = 'tcp://0.0.0.0:7890'
            else:
                docker_host = "tcp://%s:2375" % instance.ip_address

            if not hasattr(state, "docker"):
                state.docker = DockerDaemon(host=docker_host)
        await collection.map(setup_docker)

    @staticmethod
    def not_responding_instances(collection):
        return [x for x in collection.instances
                if not x.state.docker.responded]

    async def wait(self, collection, interval=60, timeout=600):
        """Waits till docker is available on every instance in the
        collection."""
        end = time.time() + timeout

        not_responded = self.not_responding_instances(collection)

        def get_container(inst):
            try:
                inst.state.docker.get_containers()
                inst.state.docker.responded = True
            except DOCKER_RETRY_EXC:
                logger.debug("Docker not ready yet on %s",
                             str(inst.instance.id))
            except Exception as exc:
                logger.debug("Got exception on %s: %r",
                             str(inst.instance.id), exc)

        # Attempt to fetch until they've all responded
        while not_responded and time.time() < end:
            await gen.multi([collection.execute(get_container, x)
                             for x in not_responded])

            # Update the not_responded
            not_responded = self.not_responding_instances(collection)

            if not_responded:
                await collection.wait(interval)

        # Prune the non-responding
        logger.debug("Pruning %d non-responding instances.",
                     len(not_responded))
        await collection.remove_instances(not_responded)

    async def is_running(self, collection, container_name, prune=True):
        """Checks running instances in a collection to see if the provided
        container_name is running on the instance."""
        def has_container(instance):
            try:
                all_containers = instance.state.docker.get_containers()
            except:
                if prune:
                    msg = ("Lost contact with a container on %s, "
                           "marking dead.")
                    logger.debug(msg % instance.instance.id)
                    instance.state.nonresponsive = True
                return not prune
            return any(container_name in cont["Image"]
                       for cont in all_containers.values())

        results = await gen.multi([collection.execute(has_container, x)
                                   for x in collection.running_instances()])
        return any(results)

    async def load_containers(self, collection, container_name, container_url):
        """Loads's a container of the provided name to the instance."""
        @retry(on_result=lambda res: not res)
        def image_loaded(docker, container_name):
            return docker.has_image(container_name)

        def load(instance):
            def debug(msg):
                logger.debug("[%s] %s" % (instance.instance.id, msg))

            docker = instance.state.docker

            has_container = docker.has_image(container_name)
            if has_container and "latest" not in container_name:
                return

            if container_url:
                debug("Importing %s" % container_url)
                with self.sshclient.connect(instance.instance) as client:
                    output = docker.import_container(client, container_url)
                    if output:
                        logger.debug(output)
            else:
                debug("Pulling %r" % container_name)
                output = docker.pull_container(container_name)

            if not image_loaded(docker, container_name):
                debug("Docker does not have %s" % container_name)
                return False
            return output

        await collection.map(load)

    async def run_containers(self,
                             collection: EC2Collection,
                             name: str,
                             command: Optional[str] = None,
                             env: Optional[Dict[str, str]] = None,
                             volumes={},
                             ports={},
                             local_dns=None,
                             delay=0,
                             pid_mode=None):
        """Run a container of the provided name with the env/command
        args supplied."""
        if env is None:
            env = {}

        if local_dns is not None:
            local_dns = collection.local_dns

        if isinstance(ports, str):
            port_list = [x.split(":") for x in ports.split(",")]
            ports = {x[0]: x[1] for x in port_list if x and len(x) == 2}

        if isinstance(volumes, str):
            volume_list = [x.split(":") for x in volumes.split(",")]
            volumes = {x[1]: {"bind": x[0], "ro": len(x) < 3 or x[2] == "ro"}
                       for x in volume_list if x and len(x) >= 2}

        def run(instance, tries=0):
            dns = getattr(instance.state, "dns_server", [])
            docker = instance.state.docker
            rinstance = instance.instance

            extra = [
                ("HOST_IP", rinstance.ip_address),
                ("PRIVATE_IP", rinstance.private_ip_address),
                ("STATSD_HOST", rinstance.private_ip_address),
                ("STATSD_PORT", "8125")]
            extra_env = env.copy()
            extra_env.update(extra)
            _env = {self.substitute_names(k, extra_env):
                    self.substitute_names(v, extra_env)
                    for k, v in extra_env.items()}

            if command is None:
                _command = None
            else:
                _command = self.substitute_names(command, _env)

            _volumes = {}
            for host, volume in volumes.items():
                binding = volume.copy()
                binding["bind"] = self.substitute_names(
                    binding.get("bind", host), _env)
                _volumes[self.substitute_names(host, _env)] = binding

            try:
                return docker.run_container(
                    name,
                    _command,
                    env=_env,
                    volumes=_volumes,
                    ports=ports,
                    dns=dns,
                    pid_mode=pid_mode)
            except Exception as exc:
                logger.debug("Exception with run_container: %s", exc)
                if tries > 3:
                    logger.debug("Giving up on running container.")
                    return False
                docker.stop_container(name)
                return run(instance, tries=tries+1)
        results = await collection.map(run, delay=delay)
        return results

    async def kill_containers(self, collection, container_name):
        """Kill the container with the provided name."""
        def kill(instance):
            try:
                instance.state.docker.kill_container(container_name)
            except Exception:
                logger.debug("Lost contact with a container, marking dead.",
                             exc_info=True)
                instance.state.nonresponsive = True
        await collection.map(kill)

    async def stop_containers(self,
                              collection,
                              container_name,
                              timeout=15,
                              capture_stream=None):
        """Gracefully stops the container with the provided name and
        timeout."""
        def stop(instance):
            try:
                instance.state.docker.stop_container(
                    container_name,
                    timeout,
                    capture_stream)
            except Exception:
                logger.debug("Lost contact with a container, marking dead.",
                             exc_info=True)
                instance.state.nonresponsive = True
        await collection.map(stop)

    @staticmethod
    def substitute_names(tmpl_string, dct):
        """Given a template string, sub in values from the dct"""
        return Template(tmpl_string).substitute(dct)


class Heka:
    """Heka additions to AWS instances"""
    def __init__(self, info, ssh, options, influx):
        self.info = info
        self.sshclient = ssh
        self.options = options
        self.influx = influx

    async def start(self,
                    collection,
                    docker,
                    ping,
                    database_name,
                    series=None):
        """Launches Heka containers on all instances."""
        if not self.options:
            logger.debug("Heka not configured")
            return

        volumes = {
            '/home/core/heka': {'bind': '/heka', 'ro': False},
            # '/proc': {'bind': '/proc', 'ro': False}
        }
        ports = {(8125, "udp"): 8125, 4352: 4352}

        series_name = ""
        if series:
            series_name = "%s." % series

        # Upload heka config to all the instances
        def upload_files(inst):
            hostname = "%s%s" % (
                series_name,
                inst.instance.ip_address.replace('.', '_')
            )
            if self.influx:
                config_file = HEKA_CONFIG_TEMPLATE.substitute(
                    remote_addr=join_host_port(self.options.host,
                                               self.options.port),
                    remote_secure=self.options.secure and "true" or "false",
                    influx_addr=join_host_port(self.influx.host,
                                               self.influx.port),
                    influx_db=database_name,
                    hostname=hostname)
            else:
                config_file = HEKA_NOINFLUX_TEMPLATE.substitute(
                    remote_addr=join_host_port(self.options.host,
                                               self.options.port),
                    remote_secure=self.options.secure and "true" or "false",
                    hostname=hostname)
            with StringIO(config_file) as fl:
                self.sshclient.upload_file(inst.instance, fl,
                                           "/home/core/heka/config.toml")
        await collection.map(upload_files)

        logger.debug("Launching Heka...")
        await docker.run_containers(collection, self.info.name,
                                    "hekad -config=/heka/config.toml",
                                    volumes=volumes, ports=ports,
                                    pid_mode="host")

        await gen.multi(
            [ping.ping("http://%s:4352/" % inst.instance.ip_address)
             for inst in collection.instances])

    async def stop(self, collection, docker):
        await docker.stop_containers(collection, self.info.name)


class DNSMasq:
    """Manages DNSMasq on AWS instances."""
    def __init__(self, info, docker):
        self.info = info
        self.docker = docker

    async def start(self, collection, hostmap):
        """Starts dnsmasq on a host with a given host mapping.

        Host mapping is a dict of "Hostname" -> ["IP"].

        """
        records = []
        tmpl = Template("--host-record=$name,$ip")
        for name, ips in hostmap.items():
            for ip in ips:
                records.append(tmpl.substitute(name=name, ip=ip))

        cmd = "/usr/sbin/dnsmasq -k " + " ".join(records)
        ports = {(53, "udp"): 53}

        results = await self.docker.run_containers(
            collection, self.info.name, cmd, ports=ports, local_dns=False)

        # Add the dns info to the instances
        for inst, response in zip(collection.instances, results):
            state = inst.state
            if hasattr(state, "dns_server"):
                continue
            dns_ip = response["NetworkSettings"]["IPAddress"]
            state.dns_server = dns_ip

    async def stop(self, collection):
        await self.docker.stop_containers(collection, self.info.name)


class Watcher:
    """Watcher additions to AWS instances"""
    def __init__(self, info, options=None):
        self.info = info
        self.options = options

    async def start(self, collection, docker):
        """Launches Heka containers on all instances."""
        if not self.options:
            logger.debug("Watcher not configured")
            return

        bind = {'bind': '/var/run/docker.sock', 'ro': False}
        volumes = {'/var/run/docker.sock': bind}
        ports = {}
        env = {'AWS_ACCESS_KEY_ID': self.options['AWS_ACCESS_KEY_ID'] or "",
               'AWS_SECRET_ACCESS_KEY':
               self.options['AWS_SECRET_ACCESS_KEY'] or ""}

        logger.debug("Launching Watcher...")
        await docker.run_containers(collection, self.info.name,
                                    "python ./watch.py", env=env,
                                    volumes=volumes, ports=ports,
                                    pid_mode="host")

    async def stop(self, collection, docker):
        await docker.stop_containers(collection, self.info.name)


class ContainerInfo(namedtuple("ContainerInfo",
                               "name url")):
    """Named tuple containing container information."""
