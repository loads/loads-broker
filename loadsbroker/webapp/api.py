"""HTTP APIs

URL layout:

``/api`` -> :class:`~RootHandler`

``/api/run/*`` -> :class:`~RunHandler`

``/api/orchestrate/*`` -> :class:`~OrchestrateHandler`

``/dashboards/run/RUN_ID/`` ->
:class:`~loadsbroker.webapp.views.GrafanaHandler`

"""
import json
import os

import tornado.web
from sqlalchemy.orm.exc import NoResultFound

from loadsbroker import __version__, logger
from loadsbroker.db import Run, COMPLETED
from loadsbroker.exceptions import LoadsException


_DEFAULTS = {'user_data': os.path.join(os.path.dirname(__file__), 'aws.yml')}


class BaseHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, **kw):
        super(BaseHandler, self).__init__(application, request, **kw)
        self.broker = application.broker
        self.db = self.broker.db

    def _get_run(self, run_id):
        session = self.db.session()
        try:
            run = session.query(Run).filter(Run.uuid == run_id).one()
        except NoResultFound:
            run = None
        return run, session

    def _handle_request_exception(self, e):
        logger.error(e)
        self.write_error(status=500, message=str(e))

    def set_default_headers(self):
        """Set the header to JSON"""
        self.set_header('Content-Type', 'application/json')

    def write_error(self, status=400, **kw):
        self.set_status(status)
        if 'message' not in kw:
            if status == 405:
                kw['message'] = 'Invalid HTTP method.'
            else:
                kw['message'] = 'Unknown error.'

        self.response = kw
        self.write_json()

    def prepare(self):
        if self.request.body:
            body = self.request.body.decode('utf8')
            try:
                json_data = json.loads(body)
                self.request.arguments.update(json_data)
            except ValueError as e:
                self.send_error(400, message=str(e))

        self.response = {}

    def write_json(self):
        status = self.get_status()
        if 'success' not in self.response:
            self.response['success'] = status <= 299
        self.response['status'] = status
        output = json.dumps(self.response)
        self.write(output)
        self.finish()


class RootHandler(BaseHandler):
    """Root API handler"""
    def get(self):
        """Returns the version, and current runs in progress."""
        self.response['version'] = __version__
        # XXX batching, filtering...
        self.response['projects'] = self.broker.get_projects()
        self.response['plans'] = self.broker.get_plans()
        self.response['runs'] = self.broker.get_runs()
        self.write_json()


class ProjectHandler(BaseHandler):
    def get(self, project_id):
        project, _ = self.broker._get_project(project_id)

        if project is None:
            self.write_error(status=404, message='No such project')
            return

        self.response = {'project': project.json()}
        self.write_json()


class PlanHandler(BaseHandler):
    def get(self, plan_id):
        plan, _ = self.broker._get_plan(plan_id)

        if plan is None:
            self.write_error(status=404, message='No such plan')
            return

        self.response = {'plan': plan.json()}
        self.write_json()


class RunHandler(BaseHandler):
    """Run API handler"""
    def delete(self, run_id):
        """Deleting a run does the following:
            - stops everything running
            - move the status to TERMINATED

        The Run itself is not removed from the Database.

        If the Run is already TERMINATED, returns a 400.
        If the Run does not exist, returns a 404
        """
        purge = self.get_argument('purge', False)
        run, session = self._get_run(run_id)

        if run is None:
            self.write_error(status=404, message='No such run')
            return

        if run.state == COMPLETED and not purge:
            self.write_error(status=400, message='Already terminated')
            return

        # 1. stop any activity
        self.response['stopped_running'] = self.broker.abort_run(run_id)

        # 2. set the status to TERMINATED - or delete the run
        if not purge:
            run.state = COMPLETED
        else:
            self.broker.delete_run(run_id)
        self.write_json()

    def get(self, run_id):
        """Returns the Run

        If that run does not exists, returns a 404.
        """
        run, __ = self._get_run(run_id)

        if run is None:
            self.write_error(status=404, message='No such run')
            return

        self.response = {'run': run.json()}
        self.write_json()


class OrchestrateHandler(BaseHandler):
    """Orchestration API handler"""
    def post(self, strategy_id, **additional_kwargs):
        """Start a strategy running.

        Any additional key/value's passed in are made available for
        variable interpolation in the container sets for interpolated
        options.

        ``run_uuid`` can be passed in, and will be set as the run_uuid
        for this run. Care should be taken to ensure this is a random
        UUID that won't conflict or an error will occur.

        ``create_db`` can be passed in, and should be set to ``0`` if
        an InfluxDB database should not be created for this run. If the
        broker doesn't create it, some other process should have created
        the database and passed in ``run_uuid`` as well.

        """
        result = {"success": True}
        create_db = additional_kwargs.pop("create_db", "1") == "1"
        try:
            result["run_id"] = self.broker.run_plan(
                strategy_id, create_db, **additional_kwargs)
        except LoadsException:
            self.write_error(status=404, message="No such strategy.")
            return
        except:
            logger.exception("Error handling post")

        self.response = result
        self.write_json()

    def delete(self, run_id):
        """Abort an existing run."""
        self.response = result = {}
        result["success"] = self.broker.abort_run(run_id)
        self.write_json()
