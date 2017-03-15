"""Database layer

"""
import datetime
import json
from collections import OrderedDict
from uuid import uuid4

import toml

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
    subqueryload,
)
from sqlalchemy.pool import StaticPool
from sqlalchemy.types import TypeDecorator

from loadsbroker import logger
from loadsbroker.exceptions import LoadsException


def suuid4():
    return str(uuid4())


class JSONEncodedDict(TypeDecorator):
    """Represents an immutable structure as a JSON-encoded string."""

    impl = String

    def process_bind_param(self, value, dialect):
        return value if value is None else json.dumps(value)

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        return json.loads(value, object_pairs_hook=OrderedDict)


class Base:
    """Base SQLAlchemy class"""
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()

    id = Column(Integer, primary_key=True, autoincrement=True)
    uuid = Column(String, default=suuid4)

    def json(self, fields=None):
        """Attempt to set the SQLAlchemy table with the keys from
        the JSON as the columns."""
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

    def _datetostr(self, date):
        if date is None:
            return None
        return date.isoformat()


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
    """Converts status states to an output-friendly format"""
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
    """A Project contains all the load-test plans available for this
    project.

    Projects can have multiple plans for a variety of load-tests.

    """
    name = Column(String, doc="Name of the project")
    home_page = Column(String, nullable=True, doc="Project home-page")

    plans = relationship("Plan", backref="project")

    def json(self, fields=None):
        return {'uuid': self.uuid, 'name': self.name,
                'home_page': self.home_page,
                'plans': [plan.json(fields) for plan in self.plans]}


class Plan(Base):
    """Load-test Plan

    Contains one or more :class:`steps <Step>` to run for the recipe.

    """
    name = Column(String, doc="Short visible name for the plan")
    description = Column(String, nullable=True,
                         doc="Detailed description of the load-test "
                             "plan")
    enabled = Column(Boolean, default=False, doc="Enable/Disable the "
                     "plan")
    project_id = Column(Integer, ForeignKey("project.id"))

    steps = relationship("Step", backref="plan")
    runs = relationship("Run", backref="plan")

    @classmethod
    def load_with_steps(cls, session, uuid):
        """Fully load a plan along with its steps"""
        return session.query(cls).\
            options(subqueryload(cls.steps)).\
            filter_by(uuid=uuid).one()

    @classmethod
    def from_json(cls, json):
        """Create a recipe from a JSON dict"""
        steps = json["steps"]
        del json["steps"]
        strategy = cls(**json)
        strategy.steps = [Step.from_json(**kw) for kw in steps]
        return strategy

    def json(self, fields=None):
        """Used to serialize the instance into JSON
        """
        return {'uuid': self.uuid, 'name': self.name,
                'description': self.description, 'enabled': self.enabled,
                'runs': [run.json(fields) for run in self.runs],
                'steps': [step.json(fields) for step in self.steps]}


class Step(Base):
    """A Step represents a single program to run, how/when/where to run
    it, and with what environment/command-line arguments.

    It represents:

    - What Container to run ('bbangert/push-tester:latest')
    - How many of them to run (200 instances)
    - What instance type to run them on ('r3.large')
    - What region the instances should be in ('us-west-2')
    - Maximum amount of time the step should run (20 minutes)
    - Delay after the run has started before this step should be run

    To run alternate configurations of these options (more/less
    instances, different instance types, regions, max time, etc.)
    additional :class:`steps <Step>` should be created for a plan.

    """
    # Basic Collection data
    name = Column(String, doc="Short description of the step")

    # Triggering data
    # XXX we need default values for all of these
    run_delay = Column(
        Integer,
        doc="Delay from start of run before the step can run.",
        default=0
    )
    run_max_time = Column(
        Integer,
        doc="How long to run this step for, in seconds.",
        default=600
    )

    # node_backoff_factor = Column(
    #     Float,
    #     doc="Backoff factor applied to delay before next node trigger."
    # )

    # AWS parameters
    instance_region = Column(Enum(name="InstanceRegion", *AWS_REGIONS),
                             default='us-west-2',
                             doc="Region to spin up instances")
    instance_type = Column(String, default='t1.micro',
                           doc="Type of instance to use")
    instance_count = Column(Integer, default=1,
                            doc="How many instances to spin up")

    # Test container run data
    container_name = Column(String, doc="Docker container name/tag to use, "
                            "ie, `bbangert/pushtester:dev`.")
    container_url = Column(String, doc="URL to retrieve the container from, "
                           "an exported docker container using `docker save`.")
    environment_data = Column(
        JSONEncodedDict, default="",
        doc="Environment data to pass to the container. *Interpolated*")
    additional_command_args = Column(String, default="", doc="Any additional "
                                     "command line argument to pass to the "
                                     "container. *Interpolated*")

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
    volume_mapping = Column(
        String,
        nullable=True,
        doc="Volumes that should be exposed to containers in this set."
    )
    docker_series = Column(
        String,
        nullable=True,
        doc="Series name to use in the cadvisor db for this set."
    )
    prune_running = Column(
        Boolean,
        default=True,
        doc="Whether non-responsive/heavily-loaded instances should be "
            "pruned."
    )
    node_delay = Column(
        Integer,
        default=0,
        doc="Delay between launching each instance in this step"
    )

    step_records = relationship("StepRecord", backref="step")

    plan_id = Column(Integer, ForeignKey("plan.id"))

    @classmethod
    def from_json(cls, **json):
        env_data = json.get("environment_data")
        if env_data and isinstance(env_data, list):
            json["environment_data"] = dict(
                line.split('=', 1) for line in env_data)
        return cls(**json)

    def json(self, fields=None):
        return {'uuid': self.uuid, 'name': self.name,
                'run_delay': self.run_delay,
                'run_max_time': self.run_max_time,
                'instance_region': self.instance_region,
                'instance_type': self.instance_type,
                'container_name': self.container_name,
                'container_url': self.container_url,
                'environment_data': self.environment_data,
                'additional_command_args': self.additional_command_args,
                'dns_name': self.dns_name,
                'port_mapping': self.port_mapping,
                'volume_mapping': self.volume_mapping,
                'docker_series': self.docker_series,
                'prune_running': self.prune_running,
                'node_delay': self.node_delay,
                'plan_id': self.plan_id,
                'instance_count': self.instance_count,
                'step_records': [rec.json(fields)
                                 for rec in self.step_records]}


class StepRecord(Base):
    """Links a :class:`Run` to a :class:`Step` to record run specific data
    for utilizing the :class:`Step`.

    This intermediary table stores a record of a Step being run to a Run,
    such as when it was created and started so that it can be determined
    when this step should be stopped.

    """
    created_at = Column(DateTime, default=datetime.datetime.utcnow,
                        doc="When the step was created.")
    started_at = Column(DateTime, nullable=True, doc="When the step was "
                        "started.")
    completed_at = Column(DateTime, nullable=True, doc="When the step was "
                          "completed or shut down.")
    failed = Column(Boolean, default=False, doc="If the step failed to start "
                    "properly.")

    run_id = Column(ForeignKey("run.id"))
    step_id = Column(ForeignKey("step.id"))

    @classmethod
    def from_step(cls, step):
        """Create a :class:`StepRecord` linked to a :class:`Step`"""
        srec = cls()
        srec.step = step
        return srec

    def should_stop(self):
        """Indicates if this step should be stopped."""
        now = datetime.datetime.utcnow()
        max_delta = datetime.timedelta(seconds=self.step.run_max_time)
        return now >= self.started_at + max_delta

    def should_start(self):
        """Indicates if this step should be started."""
        # XXX Don't return true if it should_stop.
        # Don't start more than once
        if self.started_at:
            return False
        now = datetime.datetime.utcnow()
        delay_delta = datetime.timedelta(seconds=self.step.run_delay)
        return now >= self.run.started_at + delay_delta

    def json(self, fields=None):
        return {'uuid': self.uuid, 'run_id': self.run_id,
                'step_id': self.step_id, 'failed': self.failed,
                'created_at': self._datetostr(self.created_at),
                'completed_at': self._datetostr(self.completed_at),
                'started_at': self._datetostr(self.started_at)}


class Run(Base):
    """Represents a single run of a :class:`Strategy`

    Every time a strategy is run, a :class:`Run` is created for it.
    Each run tracks the state of the running strategy, when it was
    created and started, and running container sets.

    """
    state = Column(Integer, default=INITIALIZING)

    owner = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.datetime.utcnow,
                        doc="When the Run was created.")
    started_at = Column(DateTime, nullable=True, doc="When the Run was "
                        "started.")
    completed_at = Column(DateTime, nullable=True, doc="When the Run has "
                          "finished, whether aborted or not.")
    aborted = Column(Boolean, default=False, doc="Whether the Run was "
                     "aborted.")

    step_records = relationship("StepRecord", backref="run")

    plan_id = Column(Integer, ForeignKey("plan.id"))

    @classmethod
    def new_run(cls, session, plan_uuid, owner=None):
        """Create a new run with appropriate running container set
        linkage for a given strategy"""
        plan = Plan.load_with_steps(session, plan_uuid)
        if not plan:
            raise LoadsException("Unable to locate plan: %s" % plan_uuid)

        run = cls()
        run.plan = plan
        run.owner = owner

        # Setup step records for each step in this plan
        run.step_records = [StepRecord.from_step(step) for step in plan.steps]

        return run

    def json(self, fields=None):
        return {'uuid': self.uuid, 'state': self.state,
                'aborted': self.aborted,
                'step_records': [rec.json(fields)
                                 for rec in self.step_records],
                'created_at': self._datetostr(self.created_at),
                'completed_at': self._datetostr(self.completed_at),
                'started_at': self._datetostr(self.started_at),
                'plan_id': self.plan_id, 'plan_name': self.plan.name,
                'owner': self.owner}


run_table = Run.__table__


class Database:
    """Main database object that creates the SQLAlchemy engine and session.

    Also creates the tables if passed the appropriate argument.

    """
    def __init__(self, uri, create=True, echo=False):
        if uri.startswith('sqlite'):
            args = {'check_same_thread': False}
            self.engine = create_engine(uri, connect_args=args,
                                        poolclass=StaticPool)

        else:
            self.engine = create_engine(uri)
        self.session = sessionmaker(bind=self.engine)

        # create tables
        if create:
            Base.metadata.create_all(self.engine)


def setup_database(session, db_file):
    """Helper function to setup the initial database based off a json
    file"""
    logger.debug("Verifying database setup.")
    with open(db_file) as fp:
        if db_file.lower().endswith('json'):
            data = json.load(fp, object_pairs_hook=OrderedDict)
        elif db_file.lower().endswith('toml'):
            data = toml.loads(fp.read())
        else:
            exit('ERROR: initial db file format not recogized.  Aborting!')

    # Verify the project exists
    project = session.query(Project).filter_by(name=data["name"]).first()
    if not project:
        project = Project(name=data["name"])
        session.add(project)
        session.commit()

    logger.debug("Project ID: %s", project.uuid)

    # Key plans by name to look them up quickly if they exist
    existing = {plan.name: plan for plan in project.plans}

    # Verify every strategy exists
    for plan in data["plans"]:
        ex_plan = existing.get(plan["name"])
        if ex_plan:
            logger.debug("Found plan: %s, UUID: %s", ex_plan.name,
                         ex_plan.uuid)
            continue
        new_plan = Plan.from_json(plan)
        project.plans.append(new_plan)
        session.commit()

        logger.debug("Added plan: %s, UUID: %s", new_plan.name, new_plan.uuid)
    logger.debug("Finished database setup.")
