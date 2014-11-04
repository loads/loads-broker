"""Database layer

Rough Schema:

Projects are organizations of load strategies and runs of the various
load strategies.

A Load Strategy is composed of Load Testing collection(s) that run
against a cluster to test. The load test strategy is a set of
instructions on what container sets to utilize with which parameters and
in what order, how long to conduct the load test, etc.

A Load Testing Collection is a single set of test machines in a single
region of a single instance type with a specified container to run for
the load test.

A Run is a single load-test run of a given Load Strategy.


Running a Strategy:

For a complete run, the database contains sufficient information to
construct all the instances needed, all the containers on all the
instances, apply various sets of instances to run at various times
during a load-test, etc.

- Initializing
1. Load all container sets and request collection objects from the pool
2. Wait for docker on all container sets and have all container sets pull
   the appropriate docker container

- Running
1. Start container sets, lowest order first with supplied command/env
2. Wait for delay between starting container sets
3. Monitor and stop container sets if they've exceeded their run-time

- Terminating
1. Ensure all container sets have stopped
2. Return container sets to the pool

- Completed

"""
import datetime
from uuid import uuid4

from sqlalchemy import (
    create_engine,
    Boolean,
    Column,
    DateTime,
    Enum,
    Float,
    Integer,
    String,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import (
    sessionmaker,
    relationship,
    subqueryload,
)

from loadsbroker.exceptions import LoadsException
from loadsbroker.util import dict2str


def suuid4():
    return str(uuid4())


class Base:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    id = Column(Integer, primary_key=True, autoincrement=True)

    def json(self, fields=None):
        data = {}
        for key, val in self.__dict__.items():
            if key.startswith('_'):
                continue
            if fields is not None and key not in fields:
                continue
            if isinstance(val, datetime.datetime):
                val = val.isoformat()
            data[key] = val
        return data


Base = declarative_base(cls=Base)


AWS_REGIONS = (
    "ap-northeast-1", "ap-southeast-1", "ap-southeast-2",
    "eu-west-1",
    "sa-east-1",
    "us-east-1", "us-west-1", "us-west-2"
)
INITIALIZING = 0
RUNNING = 1
TERMINATING = 2
COMPLETED = 3


def status_to_text(status):
    if status == INITIALIZING:
        return "INITIALIZING"
    elif status == RUNNING:
        return "RUNNING"
    elif status == TERMINATING:
        return "TERMINATING"
    elif status == COMPLETED:
        return "COMPLETED"
    else:
        return "UNKNOWN"


class Project(Base):
    name = Column(String)
    repository = Column(String)
    home_page = Column(String, nullable=True)

    strategies = relationship("Strategy", backref="project")


class Strategy(Base):
    name = Column(String)
    enabled = Column(Boolean, default=False)
    trigger_url = Column(String, nullable=True)
    project_id = Column(Integer, ForeignKey("project.id"))

    container_sets = relationship("ContainerSet", backref="strategy")
    runs = relationship("Run", backref="strategy")

    @classmethod
    def load_with_container_sets(cls, session, name):
        """Fully load a strategy along with its container sets"""
        return session.query(cls).\
            options(subqueryload(cls.container_sets)).\
            filter_by(name=name).one()


class ContainerSet(Base):
    """ContainerSet represents container running information for a set
    of instances.

    It represents:
    - What Container to run ('bbangert/push-tester:latest')
    - How many of them to run (200 instances)
    - What instance type to run them on ('r3.large')
    - What region the instances should be in ('us-west-2')
    - Maximum amount of time the container should run (20 minutes)
    - Delay after the run has started before this set should be run

    To run alternate configurations of these options (more/less
    instances, different instance types, regions, max time, etc.)
    additional :ref:`ContainerSet`s should be created for a
    strategy.

    """
    # Basic Collection data
    name = Column(String)
    uuid = Column(String, default=suuid4)

    # Triggering data
    # XXX we need default values for all of these
    run_delay = Column(
        Integer,
        doc="Delay from start of run before the collection can run.",
        default=0
    )
    run_max_time = Column(
        Integer,
        doc="How long to run this collection for, in seconds.",
        default=600
    )

    # XXX FIXME: Not used at the moment.
    node_delay = Column(
        Integer,
        doc=("How many ms to wait before triggering the container on the "
             "next node")
    )
    node_backoff_factor = Column(
        Float,
        doc="Backoff factor applied to delay before next node trigger."
    )

    # AWS parameters
    instance_region = Column(Enum(*AWS_REGIONS), default='us-west-2')
    instance_type = Column(String, default='t1.micro')
    instance_count = Column(Integer, default=1)

    # Test container run data
    container_name = Column(String)
    container_url = Column(String)
    environment_data = Column(String, default="")
    additional_command_args = Column(String, default="")

    # Container registration options
    dns_name = Column(
        String,
        nullable=True,
        doc="Register IP's for these instances to this DNS Name"
    )
    port_mapping = Column(
        String,
        nullable=True,
        doc="Ports that should be exposed on the main host."
    )

    running_container_sets = relationship("RunningContainerSet",
                                          backref="container_set")

    strategy_id = Column(Integer, ForeignKey("strategy.id"))


class RunningContainerSet(Base):
    """Links a :ref:`Run` to a :ref:`ContainerSet` to record run
    specific data for utilizing the :ref:`ContainerSet`.

    This intermediary table stores actual applications of a
    ContainerSet to a Run, such as when it was created and started so
    that it can be determined when this set of containers should be
    stopped.

    """
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    run_id = Column(ForeignKey("run.id"))
    container_set_id = Column(ForeignKey("containerset.id"))

    def should_stop(self):
        """Indicates if this running container set should be stopped."""
        now = datetime.datetime.utcnow()
        max_delta = datetime.timedelta(seconds=self.container_set.run_max_time)
        return now >= self.started_at + max_delta

    def should_start(self):
        """Indicates if this container set should be started."""
        # XXX Don't return true if it should_stop.
        now = datetime.datetime.utcnow()
        delay_delta = datetime.timedelta(seconds=self.container_set.run_delay)
        return now >= self.run.started_at + delay_delta


class Run(Base):
    uuid = Column(String, default=suuid4)
    state = Column(Integer, default=INITIALIZING)

    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    running_container_sets = relationship("RunningContainerSet",
                                          backref="run")

    strategy_id = Column(Integer, ForeignKey("strategy.id"))

    @classmethod
    def new_run(cls, session, strategy_name):
        """Create a new run with appropriate running container set
        linkage for a given strategy"""
        strategy = Strategy.load_with_container_sets(session, strategy_name)
        if not strategy:
            raise LoadsException("Unable to locate strategy: %s" %
                                 strategy_name)

        run = cls()
        run.strategy = strategy

        # Setup new running container sets for this strategy
        for container_set in strategy.container_sets:
            cset = RunningContainerSet()
            run.running_container_sets.append(cset)
            cset.container_set = container_set

        return run

run_table = Run.__table__


class Database:

    def __init__(self, uri, create=True, echo=False):
        self.engine = create_engine(uri)
        self.session = sessionmaker(bind=self.engine)

        # create tables
        if create:
            Base.metadata.create_all(self.engine)


def setup_database(session, **options):
    # loading all options
    curl = ""
    image_url = options.get('image_url', curl)
    instance_count = options.get('nodes', 1)
    image_name = options.get('image_name', "kitcambridge/pushtest:latest")
    strategy_name = options.get('strategy_name', 'strategic!')
    cset_name = options.get('cset_name', 'MyContainerSet')

    strategy = session.query(Strategy).filter_by(
        name=strategy_name).first()

    tc_environ = {
        "PUSH_TEST_MAX_CONNS": 10000,
        "PUSH_TEST_ADDR": "ws://testcluster.mozilla.org:8090",
        "PUSH_TEST_STATS_ADDR": "$STATSD_HOST:$STATSD_PORT"
    }

    service_environ = {
        "PUSHGO_METRICS_STATSD_HOST": "$STATSD_HOST:$STATSD_PORT",
        "PUSHGO_DISCOVERY_TYPE": "etcd",
        "PUSHGO_DISCOVERY_SERVERS":
        "http://internal-loads-test-EtcdELB-I7U9KLC25MS9-1217877132.us-east-1.elb.amazonaws.com:4001",
        "PUSHGO_DISCOVERY_DIR": "test-$RUN_ID",
    }

    if not strategy:
        # Add the test cluster service
        service = ContainerSet(name="Test Cluster",
                               instance_count=5,
                               instance_region="us-east-1",
                               instance_type="m3.medium",
                               run_max_time=60,
                               container_name="bbangert/pushgo:1.4rc2",
                               container_url="https://s3.amazonaws.com/loads-docker-images/pushgo-1.4rc2.tar.bz2",
                               environment_data=dict2str(service_environ),
                               dns_name="testcluster.mozilla.org",
                               port_mapping="8080:8090"
                               )

        # Setup the test containers
        tc = ContainerSet(name=cset_name,
                          instance_count=instance_count,
                          instance_region="us-east-1",
                          run_max_time=10,
                          run_delay=10,
                          container_name=image_name,
                          container_url=image_url,
                          environment_data=dict2str(tc_environ))

        strategy = Strategy(name=strategy_name, container_sets=[service, tc])
        session.add(strategy)
        session.commit()
