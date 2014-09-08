import tornado.ioloop

from loadsbroker.broker import Broker
from loadsbroker.api import application


def main():
    loop = tornado.ioloop.IOLoop.instance()
    application.broker = Broker(io_loop=loop)
    application.listen(8080)
    loop.start()


if __name__ == '__main__':
    main()
