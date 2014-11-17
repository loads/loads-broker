from string import Template

from tornado import gen
from tornado.web import StaticFileHandler


class GrafanaHandler(StaticFileHandler):
    def __init__(self, application, request, **kw):
        super(GrafanaHandler, self).__init__(application, request, **kw)
        self.influx_opts = application.broker.influx_options

    @gen.coroutine
    def get(self, path, include_body=True):
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
            yield self.flush()
        else:
            yield StaticFileHandler.get(self, self.path, include_body)
