import subprocess
import sys
import unittest
import requests
import time
from contextlib import contextmanager

from loadsbroker.client import Client
from loadsbroker import __version__


@contextmanager
def broker():
    cmd = 'from loadsbroker.main import main; main()'
    cmd = '%s -c "%s"' % (sys.executable, cmd)
    broker = subprocess.Popen(cmd, shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)

    # wait for the broker to be ready
    starting = time.time()
    started = False
    while time.time() - starting < 1:
        try:
            requests.get('http://127.0.0.1:8080', timeout=.1)
            started = True
            break
        except Exception as e:
            time.sleep(.1)

    if not started:
        print('Could not start the broker!')
        broker.kill()
        raise e

    yield

    try:
        broker.terminate()
    finally:
        broker.kill()


class TestClient(unittest.TestCase):

    def test_info(self):

        with broker():
            c = Client()
            res = c('info')

        self.assertEqual(res['version'], __version__)
