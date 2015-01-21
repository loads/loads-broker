"""HTTP APIs

URL layout:

``/api`` -> :class:`~RootHandler`

``/api/run/*`` -> :class:`~RunHandler`

``/api/orchestrate/*`` -> :class:`~OrchestrateHandler`

``/dashboards/run/RUN_ID/`` -> :class:`~GrafanaHandler`

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
        self.response['runs'] = self.broker.get_runs()
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
        # XXX

        # 2. set the status to TERMINATED - or delete the run
        if not purge:
            run.state = COMPLETED
        else:
            self.broker.delete_run(run_id)

        session.commit()
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
    def post(self, strategy_id):
        """Start a strategy running."""
        result = {"success": True}
        try:
            result["run_id"] = self.broker.run_strategy(strategy_id)
        except LoadsException:
            self.write_error(status=404, message="No such strategy.")
            return

        self.response = result
        self.write_json()

    def delete(self, run_id):
        """Abort an existing run."""
        self.response = result = {}
        result["success"] = self.broker.abort_run(run_id)
        self.write_json()
