"""Loads run-time extensions

These loads components are considered extensions as they extend the underlying
AWS instances to add feature support and state maintenance. This composition
avoids excessively large AWS instance classes as external objects can augment
the AWS instances as needed to retain their information.

"""
import json
import os
import time
import urllib.parse
from datetime import date
from string import Template
from typing import Dict, Optional

import paramiko.client as sshclient
from influxdb import InfluxDBClient
from tornado import gen

from loadsbroker import logger
from loadsbroker.aws import EC2Collection, EC2Instance
from loadsbroker.dockerctrl import DOCKER_RETRY_EXC, DockerDaemon
from loadsbroker.options import InfluxDBOptions
from loadsbroker.ssh import makedirs
from loadsbroker.util import join_host_port, retry

SUPPORT_DIR = os.path.join(os.path.dirname(__file__), "support")

with open(os.path.join(SUPPORT_DIR, "telegraf.conf"), "r") as f:
    TELEGRAF_CONF = f.read()

MONITOR_DASHBOARD_FN = "monitor-dashboard.json"
with open(os.path.join(SUPPORT_DIR, MONITOR_DASHBOARD_FN), "r") as f:
    MONITOR_DASHBOARD_JSON = f.read()


UPLOAD2S3_PATH = os.path.join(SUPPORT_DIR, "upload2s3.sh")


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
            instance = ec2_instance.instance
            state = ec2_instance.state
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
            dns = getattr(instance.state, "dns_server", None)
            dns = [dns] if dns else []
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
                return docker.safe_run_container(
                    name,
                    _command,
                    env=_env,
                    volumes=_volumes,
                    ports=ports,
                    dns=dns,
                    pid_mode=pid_mode
                )
            except Exception:
                return False
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


class InfluxDB:
    """A Run's managed InfluxDB"""

    def __init__(self, info, ssh: SSH, aws_creds: Dict[str, str]) -> None:
        self.info = info
        self.sshclient = ssh
        self.aws_creds = aws_creds

    async def start(self, collection: EC2Collection, options: InfluxDBOptions):
        await collection.map(self._setup_influxdb, 0, options)

    def _setup_influxdb(self, instance: EC2Instance, options: InfluxDBOptions):
        """With an already running InfluxDB, upload the backup script
        and create a Run db.

        """
        with open(UPLOAD2S3_PATH) as fp:
            self.sshclient.upload_file(
                instance.instance, fp, "/home/core/upload2s3.sh")

        args = options.client_args
        args['host'] = instance.instance.ip_address
        database = args.pop('database')
        client = InfluxDBClient(**args)
        logger.debug("Creating InfluxDB: %s", options.database_url)
        client.create_database(database)

    async def stop(self,
                   collection: EC2Collection,
                   options: InfluxDBOptions,
                   env: Dict[str, str],
                   project: str,
                   plan: str):
        """Backup the InfluxDB to s3."""
        if not (self.aws_creds.get('AWS_ACCESS_KEY_ID') or
                self.aws_creds.get('AWS_SECRET_ACCESS_KEY')):
            logger.error("Unable to upload2s3: No AWS credentials defined")
            return
        bucket = env.get('INFLUXDB_S3_BUCKET')
        if not bucket:
            logger.error("Unable to upload2s3: No INFLUXDB_S3_BUCKET defined")
            return

        db = options.database
        backup = "{:%Y-%m-%d}-{}-influxdb".format(date.today(), db)
        archive = backup + ".tar.bz2"
        cmd = """\
        influxd backup -database {db} {destdir}/{backup} && \
        tar cjvf {destdir}/{archive} -C {destdir} {backup} \
        """.format(
            db=db,
            destdir="/influxdb-backup",
            backup=backup,
            archive=archive
        )
        # wrap in a shell to chain commands in docker exec
        cmd = "sh -c '{}'".format(cmd)
        await collection.map(self._container_exec, 0, self.info.name, cmd)

        # upload2s3's ran from the host (vs the lightweight
        # influxdb-alpine container) because it requires openssl/curl
        destdir = os.path.join(project, plan)
        cmd = """\
        export AWS_ACCESS_KEY_ID={AWS_ACCESS_KEY_ID} && \
        export AWS_SECRET_ACCESS_KEY={AWS_SECRET_ACCESS_KEY} && \
        sh /home/core/upload2s3.sh {archive} {bucket} "{destdir}" \
        """.format(
            archive=os.path.join("/home/core/influxdb/backup", archive),
            bucket=bucket,
            destdir=destdir,
            **self.aws_creds
        )
        exits = await collection.map(self._ssh_exec, 0, cmd)
        url = "https://{}.s3.amazonaws.com/{}/{}".format(
            bucket,
            urllib.parse.quote(destdir),
            archive)
        if any(exits):
            logger.error("InfluxDB upload2s3 failed: %s (%s)", exits, url)
        else:
            logger.debug("InfluxDB upload2s3 succeeded (%s)", url)

    def _container_exec(self,
                        instance: EC2Instance,
                        container_name: str,
                        cmd: str) -> bytes:
        conts = list(instance.state.docker.containers_by_name(container_name))
        if not conts:
            return None
        cont = conts[0]  # assume 1
        return instance.state.docker.exec_run(cont['Id'], cmd)

    def _ssh_exec(self, instance: EC2Instance, cmd: str) -> int:
        with self.sshclient.connect(instance.instance) as client:
            stdin, stdout, stderr = client.exec_command(cmd)
            stdin.close()
            status = stdout.channel.recv_exit_status()
            if status:
                logger.error("ssh cmd failed:\n%s", stderr.read())
            return status


class Grafana:
    """Grafana monitor Dashboard for AWS instances"""

    data_source_defaults = dict(
        type='influxdb',
        access='proxy',
        isDefault=True,
        basicAuth=False
    )

    def __init__(self, info) -> None:
        self.info = info

    async def start(self,
                    collection: EC2Collection,
                    run_id: str,
                    options: InfluxDBOptions):
        data_source = self.data_source_defaults.copy()
        data_source.update(
            name="loads-broker InfluxDB Monitor (run_id: {})".format(run_id),
            url="http://" + join_host_port(options.host, options.port),
            database=options.database,
        )

        port = 8080
        ports = {3000: port}

        cmd = """\
        apt-get update -qq && \
        apt-get install -qq -y --no-install-recommends curl && \
        /etc/init.d/grafana-server start && \
        until curl "${__LOADS_GRAFANA_URL__}" \
                   -X POST \
                   -H "Accept: application/json" \
                   -H "Content-Type: application/json" \
                   --data-binary "${__LOADS_GRAFANA_DS_PAYLOAD__}"; do
            sleep 1
        done && \
        /etc/init.d/grafana-server stop && \
        mkdir "${GF_DASHBOARDS_JSON_PATH}" && \
        echo "${__LOADS_GRAFANA_DASHBOARD__}" >> \
             "${GF_DASHBOARDS_JSON_PATH}/monitor-dashboard.json" && \
        ./run.sh
        """
        cmd = "sh -c '{}'".format(cmd)

        # Avoid docker.run_container: it munges our special env
        def run(instance, tries=0):
            docker = instance.state.docker
            url = "http://admin:admin@localhost:3000/api/datasources"
            env = {
                'GF_DEFAULT_INSTANCE_NAME': instance.instance.id,
                'GF_DASHBOARDS_JSON_ENABLED': "true",
                'GF_DASHBOARDS_JSON_PATH': "/var/lib/grafana/dashboards",
                '__LOADS_GRAFANA_URL__': url,
                '__LOADS_GRAFANA_DS_PAYLOAD__': json.dumps(data_source),
                '__LOADS_GRAFANA_DASHBOARD__': MONITOR_DASHBOARD_JSON,
            }
            try:
                docker.safe_run_container(
                    self.info.name,
                    entrypoint=cmd,
                    env=env,
                    ports=ports,
                )
            except Exception:
                return False
            # XXX: not immediately available
            logger.info("Setting up Dashboard: http://%s:%s/dashboard/file/%s",
                        instance.instance.ip_address,
                        port,
                        MONITOR_DASHBOARD_FN)
        await collection.map(run)

    async def stop(self, collection, docker):
        await docker.stop_containers(collection, self.info.name)


class Telegraf:
    """Telegraf monitor for AWS instances"""

    def __init__(self, info) -> None:
        self.info = info

    async def start(self,
                    collection: EC2Collection,
                    _: Docker,
                    options: InfluxDBOptions,
                    step: str,
                    type_: Optional[str] = None):
        ports = {(8125, "udp"): 8125}

        cmd = """\
        echo "${__LOADS_TELEGRAF_CONF__}" > /etc/telegraf/telegraf.conf && \
        telegraf \
        """
        cmd = "sh -c '{}'".format(cmd)

        # Avoid docker.run_container: it munges our special env
        def run(instance, tries=0):
            docker = instance.state.docker
            env = {
                '__LOADS_TELEGRAF_CONF__': TELEGRAF_CONF,
                '__LOADS_INFLUX_ADDR__':
                join_host_port(options.host, options.port),
                '__LOADS_INFLUX_DB__': options.database,
                '__LOADS_TELEGRAF_HOST__': instance.instance.id,
                '__LOADS_TELEGRAF_STEP__': step
            }
            if type_:
                env['__LOADS_TELEGRAF_TYPE__'] = type_
            try:
                return docker.safe_run_container(
                    self.info.name,
                    cmd,
                    env=env,
                    ports=ports,
                )
            except Exception:
                return False
        await collection.map(run)

    async def stop(self, collection, docker):
        await docker.stop_containers(collection, self.info.name)
