#
# HTTP APIs
#
import os
import tornado.web
from loadsbroker import __version__


# CoreOS-stable-367.1.1
COREOS_IMG = 'ami-3193e801'
USER_DATA = os.path.join(os.path.dirname(__file__), 'aws.yml')


class RootHandler(tornado.web.RequestHandler):
    def get(self):
        self.write({'version': __version__})

    def post(self):
        # run a new test
        uuid = self.application.broker.run_test(ami=COREOS_IMG,
                                                user_data=USER_DATA,
                                                nodes=1)
        self.write({'run_id': uuid})


# TODO / db queries
class RunHandler(tornado.web.RequestHandler):

    def delete(self, run_id):
        self.write({'result': 'OK'})

    def get(self, run_id):
        self.write({'status': 'OK'})


application = tornado.web.Application([
    (r"/", RootHandler),
    (r"/run/(.*)", RunHandler)
])
