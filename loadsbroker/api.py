#
# HTTP APIs
#
import os
import tornado.web
from loadsbroker import __version__
from loadsbroker.db import Run


# CoreOS-stable-367.1.1
COREOS_IMG = 'ami-3193e801'
USER_DATA = os.path.join(os.path.dirname(__file__), 'aws.yml')


class RootHandler(tornado.web.RequestHandler):
    def get(self):
        runs = self.application.broker.get_runs()
        self.write({'version': __version__,
                    'runs': runs})

    def post(self):
        # run a new test
        uuid = self.application.broker.run_test(ami=COREOS_IMG, user_data=USER_DATA,
                                                nodes=15)
        self.write({'uuid': uuid})


application = tornado.web.Application([
    (r"/", RootHandler),
])
