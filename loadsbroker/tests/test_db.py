import unittest
from loadsbroker.db import Project, Plan, Step, Database


class DatabaseTest(unittest.TestCase):

    def setUp(self):
        self.db = Database('sqlite:///:memory:')

    def test_project(self):
        session = self.db.session()

        # a project is defined by a name, a repo and strategies
        project = Project(
            name='simplepush',
            home_page='https://services.mozilla.com')

        session.add(project)

        plan = Plan(name='s1', enabled=True)
        project.plans.append(plan)

        # Attach a container set to the strategy
        cset = Step(
            name="Awesome load-tester",
            instance_type="t2.micro",
            instance_count=5,
            container_name="bbangert/simpletest:latest",
            additional_command_args="--target=svc.dev.mozilla.com"
        )
        plan.steps.append(cset)

        session.commit()
