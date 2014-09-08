import tornado.ioloop

from .broker import Broker
from .api import application


def main():
    loop = tornado.ioloop.IOLoop.instance()
    application.broker = Broker(io_loop=loop)
    application.listen(8080)
    loop.start()


if __name__ == '__main__':
    main()
