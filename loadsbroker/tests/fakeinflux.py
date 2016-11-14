# see http://influxdb.com/docs/v0.8/api/reading_and_writing_data.html
import tornado.ioloop
import tornado.web
import logging
import json

logger = logging.getLogger('docker')


class BaseHandler(tornado.web.RequestHandler):
    def _handle_request_exception(self, e):
        logger.error(e)
        self.write_error(status=500, message=str(e))

    def set_default_headers(self):
        self.set_header('Content-Type', 'application/json')

    def write_error(self, status=400, **kw):
        self.set_status(status)
        if 'message' not in kw:
            if status == 405:
                kw['message'] = 'Invalid HTTP method.'
            else:
                kw['message'] = 'Unknown error.'

        self.response = kw
        self.write_json()

    def prepare(self):
        print(self.request.method + ' ' + self.request.uri)
        if self.request.body:
            body = self.request.body.decode('utf8')
            try:
                json_data = json.loads(body)
                self.request.arguments.update(json_data)
            except ValueError as e:
                self.send_error(400, message=str(e))

        self.response = {}

    def write_json(self):
        output = json.dumps(self.response)
        self.write(output)
        self.finish()


class DatabasesHandler(BaseHandler):
    def post(self):
        """Database creation
        """
        self.set_status(201)
        self.write('')
        self.finish()


class DatabaseHandler(BaseHandler):
    def delete(self):
        """Database deletion
        """
        self.set_status(204)
        self.write('')
        self.finish()


class CatchAll(BaseHandler):
    def get(self):
        self.write_json()


application = tornado.web.Application([
    (r"/db", DatabasesHandler),
    (r"/db/.*", DatabaseHandler),
    (".*", CatchAll)
])


def main():
    application.listen(8086)
    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
