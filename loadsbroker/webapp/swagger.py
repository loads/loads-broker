from functools import wraps
from yaml import safe_dump
import json
from loadsbroker import __version__
from collections import defaultdict


_DEFAULT = {
  'swagger': '2.0',
  'schemes': ['http'],
  'produces': ['application/json'],
  'consumes': ['application/json'],
  'info': {'title': 'Loads Broker',
           'description': 'The broker part of the loads project',
           'contact': {'name': 'Tarek Ziadé',
                       'email': 'tarek@mozilla.com'},
           'license': {'name': 'APLv2',
                       'url':
                       'https://www.apache.org/licenses/LICENSE-2.0.html'},
           'version': __version__,
           'x-mozilla-services': {
               'homepage': 'https://github.com/loads/loads-broker'},
           'fullEndpointsDescription': True}
  }


class SwaggerSpec:
    def __init__(self, spec_dict=None, path_prefix=None):
        self.path_prefix = path_prefix
        if spec_dict is None:
            self.spec_dict = {}
        else:
            self.spec_dict = spec_dict
        if 'paths' not in self.spec_dict:
            self.spec_dict['paths'] = defaultdict(dict)

    def yaml(self):
        return safe_dump(self.spec_dict)

    def json(self):
        return json.dumps(self.spec_dict)

    def _join(self, prefix, endpoint):
        if not prefix.startswith('/'):
            prefix = '/' + prefix
        if not prefix.endswith('/') and not endpoint.startswith('/'):
            endpoint = '/' + endpoint
        return prefix + endpoint

    def operation(self, endpoint, method, operationId, **info):
        if self.path_prefix:
            endpoint = self._join(self.path_prefix, endpoint)
        endpoint = self.spec_dict['paths'][endpoint]
        endpoint[method] = {'operationId': operationId}
        endpoint[method].update(info)

        def _op(func):
            @wraps(func)
            def __op(*args, **kw):
                return func(*args, **kw)
            return __op
        return _op


spec = SwaggerSpec(_DEFAULT, path_prefix='/api')
