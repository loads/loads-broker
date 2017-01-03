from string import Template

from tornado.web import StaticFileHandler


class GrafanaHandler(StaticFileHandler):
    """Grafana page handler"""
    def __init__(self, application, request, **kw):
        super(GrafanaHandler, self).__init__(application, request, **kw)
        self.broker = application.broker

    def _get_run(self, run_id):
        from loadsbroker.db import Run
        from sqlalchemy.orm.exc import NoResultFound
        session = self.broker.db.session()
        try:
            run = session.query(Run).filter(Run.uuid == run_id).one()
        except NoResultFound:
            run = None
        return run, session

    async def get(self, path, include_body=True):
        run_id, path = self.path_args
        if not path:
            path = "index.html"
            include_body = True

        run, _ = self._get_run(run_id)
        mgr = self.broker._runs[run.uuid]
        influxdb_options = mgr.influxdb_options
        if not influxdb_options:
            # XXX: guard against not ready yet
            pass

        absolute_path = self.get_absolute_path(self.root, path)
        if absolute_path.endswith("config.js"):
            opts = dict(INFLUX_HOST=influxdb_options.host,
                        INFLUX_USER=influxdb_options.user or "",
                        INFLUX_PASSWORD=influxdb_options.password or "",
                        RUN_ID=run_id)
            with open(absolute_path) as f:
                tmpl = Template(f.read())
            content = tmpl.substitute(opts)
            self.set_status(200)
            self.set_header("Content-Type", "application/json")
            self.set_header("Content-Length", len(content))
            self.write(content)
            await self.flush()
        else:
            await StaticFileHandler.get(self, path, include_body)
