import os

from tornado.testing import AsyncTestCase


endpoints = os.path.join(os.path.dirname(__file__),
                         'endpoints.json')
os.environ['BOTO_ENDPOINTS'] = endpoints


class TestRunManager(AsyncTestCase):
    pass
