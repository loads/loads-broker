import os


# by default, we're using the .boto file and the endpoints
# located in the tests.
#
# If you need to reset boto convfiguration, look at:
#
# - loadsbroker.tests.util.clear_boto_config
# - loadsbroker.tests.util.load_boto_config
#
BOTO_CONFIG = os.path.join(os.path.dirname(__file__), '.boto')
os.environ['BOTO_CONFIG'] = BOTO_CONFIG

endpoints = os.path.join(os.path.dirname(__file__), 'endpoints.json')
os.environ['BOTO_ENDPOINTS'] = endpoints

