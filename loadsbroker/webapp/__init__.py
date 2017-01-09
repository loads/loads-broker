import os
import tornado.web
from loadsbroker.webapp.api import (
    RootHandler,
    RunHandler,
    ProjectsHandler,
    InstancesHandler,
    InstanceHandler,
    ProjectHandler,
    OrchestrateHandler
)
from loadsbroker.webapp.views import GrafanaHandler, SwaggerHandler


_GRAFANA = os.path.join(os.path.dirname(__file__), 'grafana')


application = tornado.web.Application([
    (r"/api", RootHandler),
    (r"/api/instances", InstancesHandler),
    (r"/api/instances/(.*)", InstanceHandler),
    (r"/api/run/(.*)", RunHandler),
    (r"/api/project", ProjectsHandler),
    (r"/api/project/(.*)", ProjectHandler),
    (r"/api/orchestrate/(.*)", OrchestrateHandler),
    (r"/__api__", SwaggerHandler),
    (r"/dashboards/run/([^\/]+)/(.*)", GrafanaHandler,
     {"path": _GRAFANA, "default_filename": "index.html"})
])
