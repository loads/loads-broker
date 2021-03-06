"""Management of Step Container lifetimes"""
from pprint import pformat
from typing import Any, Dict
from typing import Optional  # noqa

from attr import attrib, attrs
from tornado import gen

from loadsbroker import db  # noqa
from loadsbroker import logger
from loadsbroker.aws import EC2Collection  # noqa
from loadsbroker.options import InfluxDBOptions  # noqa


@attrs
class ContainerInfo:
    """Container information"""
    name = attrib()  # type: str
    url = attrib()  # type: Optional[str]


S3_ROOT = "https://s3.amazonaws.com/loads-docker-images/"


WATCHER_INFO = ContainerInfo(
    "loads/loadswatch:0.1.0",
    S3_ROOT + "loadswatch-0.1.0.tar.bz2")

DNSMASQ_INFO = ContainerInfo(
    "andyshinn/dnsmasq:2.76",
    S3_ROOT + "dnsmasq-2.76.tar.bz2")

INFLUXDB_INFO = ContainerInfo(
    "influxdb:1.2-alpine",
    S3_ROOT + "influxdb-1.2-alpine.tar.bz2")

TELEGRAF_INFO = ContainerInfo(
    "telegraf:1.2-alpine",
    S3_ROOT + "telegraf-1.2-alpine.tar.bz2")

GRAFANA_INFO = ContainerInfo(
    "grafana/grafana:4.1.2",
    S3_ROOT + "grafana-4.1.2.tar.bz2")


@attrs
class StepRecordLink:
    """Links a Step/StepRecord to an EC2Collection"""

    step = attrib()  # type: db.Step
    step_record = attrib()  # type: db.StepRecord
    ec2_collection = attrib()  # type: EC2Collection
    state_description = attrib(default="")  # type: str

    base_containers = [DNSMASQ_INFO, WATCHER_INFO]

    async def initialize(self, docker):
        """Prepare the collection for containers"""
        self.state_description = "Waiting for running instances."
        await self.ec2_collection.wait_for_running()
        await self._start_docker(docker)

    async def _start_docker(self, docker):
        self.state_description = "Waiting for docker"
        await docker.setup_collection(self.ec2_collection)
        await docker.wait(self.ec2_collection, timeout=360)

        self.state_description = "Pulling container images"
        run = self.step_record.run
        containers = self.base_containers[:]
        if self.is_monitored:
            containers.append(TELEGRAF_INFO)
        containers.append(ContainerInfo(
            run.interpolate(self.step.container_name,
                            self.step.environment_data),
            run.interpolate(self.step.container_url,
                            self.step.environment_data)
        ))
        await gen.multi([
            docker.load_containers(self.ec2_collection,
                                   container.name,
                                   container.url)
            for container in containers])

    async def start(self, helpers, dns_map, influxdb_options):
        if self.base_containers:
            await self._start_base_containers(
                helpers, dns_map, influxdb_options)
        await self._start_step_containers(helpers.docker)

    async def stop(self, helpers):
        await self._stop_step_containers(helpers.docker)
        if self.base_containers:
            await self._stop_base_containers(helpers)

    async def _start_base_containers(self, helpers, dns_map, influxdb_options):
        # Reload sysctl because coreos doesn't reload this right
        await helpers.ssh.reload_sysctl(self.ec2_collection)

        # Start Watcher
        await helpers.watcher.start(self.ec2_collection, helpers.docker)

        if self.is_monitored:
            await helpers.telegraf.start(
                self.ec2_collection,
                helpers.docker,
                influxdb_options,
                step=self.step.name,
                type_=self.step.docker_series
            )

        # Startup local DNS if needed
        if self.ec2_collection.local_dns:
            logger.debug("Starting up DNS")
            await helpers.dns.start(self.ec2_collection, dns_map)

    async def _stop_base_containers(self, helpers):
        if self.is_monitored:
            await helpers.telegraf.stop(self.ec2_collection, helpers.docker)

        # Stop watcher
        await helpers.watcher.stop(self.ec2_collection, helpers.docker)

        # Stop dnsmasq
        if self.ec2_collection.local_dns:
            await helpers.dns.stop(self.ec2_collection)

        # Remove anyone that failed to shutdown properly
        gen.convert_yielded(self.ec2_collection.remove_dead_instances())

    async def _start_step_containers(self, docker):
        """Startup the testers"""
        # XXX: run env should more likely override step env
        run = self.step_record.run
        env = run.environment_data or {}
        env.update(self.step.environment_data)
        env['CONTAINER_ID'] = self.step.uuid
        logger.debug("Starting step: %s", self.ec2_collection.uuid)
        container_name = run.interpolate(
            self.step.container_name, self.step.environment_data)
        await docker.run_containers(
            self.ec2_collection,
            container_name,
            self.step.additional_command_args,
            env=env,
            ports=self.step.port_mapping or {},
            volumes=self.step.volume_mapping or {},
            delay=self.step.node_delay,
        )

    async def _stop_step_containers(self, docker):
        """Stop the docker testing agents"""
        container_name = self.step_record.run.interpolate(
            self.step.container_name, self.step.environment_data)

        capture_stream = None
        if self.step._capture_output:
            capture_stream = open(self.step._capture_output, 'ab')
        try:
            await docker.stop_containers(
                self.ec2_collection,
                container_name,
                capture_stream=capture_stream)
        finally:
            if capture_stream:
                capture_stream.close()

    async def is_done(self, docker) -> bool:
        """Determine if finished or pending termination"""
        # If we haven't been started, we can't be done
        if not self.step_record.started_at:
            return False

        # If we're already stopped, then we're obviously done
        if self.ec2_collection.finished:
            return True

        run = self.step_record.run
        container_name = run.interpolate(
            self.step.container_name, self.step.environment_data)

        # If the collection has no instances running the container, its done
        instances_running = await docker.is_running(
            self.ec2_collection,
            container_name,
            prune=self.step.prune_running
        )
        if not instances_running:
            inst_info = []
            for inst, info in self._instance_debug_info().items():
                inst_info.append(inst)
                inst_info.append(pformat(info))
            logger.debug("No instances running, collection done.")
            logger.debug("Instance information:\n%s", '\n'.join(inst_info))
            return True

        # Remove instances that stopped responding
        await self.ec2_collection.remove_dead_instances()

        # Otherwise return whether we should be stopped
        return self.step_record.should_stop()

    @property
    def is_monitored(self):
        """Is this step is monitored:

        Run defines a MonitorStep and we're not it

        """
        monitor_step = self.step_record.run.get_monitor_step()
        return monitor_step and self.step != monitor_step

    def _instance_debug_info(self) -> Dict[str, Any]:
        """Return a dict of information describing a link's instances"""
        infos = {}
        for ec2i in self.ec2_collection.instances:
            infos[ec2i.instance.id] = info = dict(
                aws_state=ec2i.instance.state,
                broker_state=vars(ec2i.state),
                step_started_at=self.step_record.started_at,
            )

            docker = getattr(ec2i.state, 'docker', None)
            if not docker:
                continue

            try:
                containers = docker.get_containers(all=True)
            except Exception as exc:
                ps = "get_containers failed: %r" % exc
            else:
                ps = []
                for ctid, ct in containers.items():
                    try:
                        state = docker._client.inspect_container(ctid)['State']
                    except Exception as exc:
                        state = "inspect_container failed: %r" % exc
                    ct['State'] = state
                    ps.append(ct)

            info['docker_ps'] = ps
        return infos


class MonitorStepRecordLink(StepRecordLink):
    """Special Link for an MonitorStep"""

    influxdb_options = attrib(default=None)  # type: InfluxDBOptions

    base_containers = [GRAFANA_INFO]

    async def start(self, helpers, dns_map, influxdb_options):
        self.influxdb_options = influxdb_options
        await self._start_step_containers(helpers.docker)
        await helpers.influxdb.start(self.ec2_collection, influxdb_options)
        await helpers.grafana.start(
            self.ec2_collection,
            self.step_record.run.uuid,
            influxdb_options)

    async def stop(self, helpers):
        await helpers.grafana.stop(self.ec2_collection, helpers.docker)
        await helpers.influxdb.stop(
            self.ec2_collection,
            self.influxdb_options,
            self.step.environment_data,
            self.step.plan.project.name,
            self.step.plan.name)
        await self._stop_step_containers(helpers.docker)
