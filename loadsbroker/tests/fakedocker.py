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


class ContainersHandler(BaseHandler):
    def get(self):
        self.response = [{"Id": "8dfafdbc3a40",
                          "Image": "base:latest",
                          "Command": "echo 1",
                          "Created": 1367854155,
                          "Status": "Exit 0",
                          "Ports": [{"PrivatePort": 2222,
                                     "PublicPort": 3333,
                                     "Type": "tcp"}],
                          "SizeRw": 12288,
                          "SizeRootFs": 0}]
        self.write_json()

    def post(self, *args, **kw):
        self.response = {"Id": "e90e34656806",
                         "Warnings": []}
        self.write_json()


class ImagesHandler(BaseHandler):

    def get(self):
        self.response = [{"RepoTags": ["ubuntu:12.04",
                                       "ubuntu:precise",
                                       "ubuntu:latest",
                                       "bbangert/simpletest:dev"],
                          "Id": "8dbd9e39...c8318c1c",
                          "Created": 1365714795,
                          "Size": 131506275,
                          "VirtualSize": 131506275}]

        self.write_json()


class ContainerHandler(BaseHandler):
    # /start
    def post(self):
        self.write('')
        self.finish()


class CatchAll(BaseHandler):
    def get(self):
        self.write_json()


application = tornado.web.Application([
    (r"/v.*/containers/json", ContainersHandler),
    (r"/v.*/containers/create", ContainersHandler),
    (r"/v.*/containers/.*/start", ContainerHandler),
    (r"/v.*/images/json", ImagesHandler),
    (".*", CatchAll)
])


def main():
    application.listen(7890)
    try:
        tornado.ioloop.IOLoop.instance().start()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":
    main()
