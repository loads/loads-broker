"""Database layer

Rough Schema:

Projects are organizations of load strategies and runs of the various
load strategies.

A Load Strategy is composed of Load Testing collection(s) that run
against a cluster to test. The load test strategy is a set of
instructions on what collections to utilize with which parameters and
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
1. Load all collections and request collection objects from the pool
2. Wait for docker on all collections and have all collections pull
   the appropriate docker container

- Running
1. Start collections, lowest order first with supplied command/env
2. Wait for delay between starting collections
3. Monitor and stop collections if they've exceeded their run-time

- Terminating
1. Ensure all collections have stopped
2. Return collections to the pool

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
    Integer,
    String,
    ForeignKey,
)
from sqlalchemy.ext.declarative import declarative_base, declared_attr
from sqlalchemy.orm import (
    sessionmaker,
    relationship,
)


class Base:
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    id = Column(Integer, primary_key=True, autoincrement=True)

    def json(self):
        data = {}
        for key, val in self.__dict__.items():
            if key.startswith('_'):
                continue
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

    collections = relationship("Collection", backref="strategy")
    runs = relationship("Run", backref="strategy")


class Collection(Base):
    # Basic Collection data
    name = Column(String)
    uuid = Column(String, default=lambda: str(uuid4()))
    created_at = Column(DateTime, default=datetime.datetime.today)
    started_at = Column(DateTime, nullable=True)
    terminated_at = Column(DateTime, nullable=True)

    # Triggering data
    run_order = Column(Integer, doc="Order to run the test collections in.")
    run_delay = Column(
        Integer,
        doc="Delay before running collections higher in ordering."
    )
    run_max_time = Column(
        Integer,
        doc="How long to run this collection for, in seconds."
    )

    # AWS parameters
    instance_region = Column(Enum(*AWS_REGIONS))
    instance_type = Column(String)
    instance_count = Column(Integer)

    # Test container run data
    container_name = Column(String)
    environment_data = Column(String)
    additional_command_args = Column(String)

    strategy_id = Column(Integer, ForeignKey("strategy.id"))


class Run(Base):
    uuid = Column(String, default=lambda: str(uuid4()))
    state = Column(Integer, default=INITIALIZING)

    created_at = Column(DateTime, default=datetime.datetime.today)
    started_at = Column(DateTime, nullable=True)
    completed_at = Column(DateTime, nullable=True)

    project_id = Column(Integer, ForeignKey("project.id"))

    def __init__(self, *args, **kw):
        super(Run, self).__init__(*args, **kw)


run_table = Run.__table__


class Database:

    def __init__(self, uri, create=True, echo=False):
        self.engine = create_engine(uri)
        self.session = sessionmaker(bind=self.engine)

        # create tables
        if create:
            Base.metadata.create_all(self.engine)
