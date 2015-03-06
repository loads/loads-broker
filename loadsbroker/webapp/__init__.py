import os
import tornado.web
from loadsbroker.webapp.api import (
    RootHandler,
    ProjectHandler,
    PlanHandler,
    RunHandler,
    OrchestrateHandler,
)
from loadsbroker.webapp.views import GrafanaHandler


_GRAFANA = os.path.join(os.path.dirname(__file__), 'grafana')


application = tornado.web.Application([
    (r"/api", RootHandler),
    (r"/api/project/(.*)", ProjectHandler),
    (r"/api/plan/(.*)", PlanHandler),
    (r"/api/run/(.*)", RunHandler),
    (r"/api/orchestrate/(.*)", OrchestrateHandler),
    (r"/dashboards/run/([^\/]+)/(.*)", GrafanaHandler,
     {"path": _GRAFANA, "default_filename": "index.html"})
])
