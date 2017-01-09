from string import Template

from tornado.web import StaticFileHandler, RequestHandler
from loadsbroker.webapp.swagger import spec


class GrafanaHandler(StaticFileHandler):
    """Grafana page handler"""
    def __init__(self, application, request, **kw):
        super(GrafanaHandler, self).__init__(application, request, **kw)
        self.influx_opts = application.broker.influx_options

    async def get(self, path, include_body=True):
        self.run_id, self.path = self.path_args
        if not self.path:
            self.path = "index.html"
            include_body = True
        absolute_path = self.get_absolute_path(self.root, self.path)
        if absolute_path.endswith("config.js"):
            opts = dict(INFLUX_HOST=self.influx_opts.host,
                        INFLUX_USER=self.influx_opts.user,
                        INFLUX_PASSWORD=self.influx_opts.password,
                        RUN_ID=self.run_id)
            with open(absolute_path) as f:
                tmpl = Template(f.read())
            content = tmpl.substitute(opts)
            self.set_status(200)
            self.set_header("Content-Type", "application/json")
            self.set_header("Content-Length", len(content))
            self.write(content)
            await self.flush()
        else:
            await StaticFileHandler.get(self, self.path, include_body)


class SwaggerHandler(RequestHandler):
    async def get(self, *args, **kw):
        self.set_status(200)
        self.set_header("Content-Type", "application/json")

        # settings up schemes and host
        host = self.request.headers.get('X-Forwarded-Host')
        if host is None:
            host = self.request.headers.get('Host', 'localhost:8080')
        spec.spec_dict['host'] = host
        scheme = self.request.headers.get('X-Forwarded-Proto', 'http')
        spec.spec_dict['schemes'] = [scheme]

        content = spec.json()
        self.set_header("Content-Length", len(content))
        self.write(content)
        await self.flush()
