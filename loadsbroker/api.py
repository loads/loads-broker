#
# HTTP APIs
#
import json
import os

import tornado.web
from loadsbroker import __version__


_DEFAULTS = {'ami': 'ami-3193e801',
             'nodes': 1,
             'user_data': os.path.join(os.path.dirname(__file__), 'aws.yml')}


class BaseHandler(tornado.web.RequestHandler):
    def __init__(self, application, request, **kw):
        super(BaseHandler, self).__init__(application, request, **kw)
        self.broker = application.broker
        self.db = self.broker.db

    def set_default_headers(self):
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
        output = json.dumps(self.response)
        self.write(output)


class RootHandler(BaseHandler):
    def get(self):
        self.response['version'] = __version__
        self.write_json()

    def post(self):
        # need more sanitizing here
        options = dict(self.request.arguments)

        for key, val in _DEFAULTS.items():
            if key not in options:
                options[key] = val

        # run a new test
        options['run_id'] = self.broker.run_test(**options)
        self.response = options
        self.write_json()


# TODO / db queries
class RunHandler(BaseHandler):

    def delete(self, run_id):
        self.response['result'] = 'OK'
        self.write_json()

    def get(self, run_id):
        self.response['result'] = 'OK'
        self.write_json()


application = tornado.web.Application([
    (r"/", RootHandler),
    (r"/run/(.*)", RunHandler)
])
