from io import StringIO
from functools import wraps
import sys
import subprocess
import requests
import time
import os

import boto
from loadsbroker.aws import AWS_REGIONS


def create_image(region="us-west-2"):
    conn = boto.ec2.connect_to_region(region)
    reservation = conn.run_instances('ami-1234abcd')
    instance = reservation.instances[0]
    conn.create_image(instance.id, "Core OS stable")


def _start_daemon(cmd, port):
    daemon = subprocess.Popen(cmd, shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)

    # wait for the fake docker daemon to be ready
    starting = time.time()
    started = False

    errors = []

    while time.time() - starting < 2:
        try:
            requests.get('http://127.0.0.1:%d' % port, timeout=.1)
            started = True
            break
        except Exception as exc:
            errors.append(exc)
            time.sleep(.1)

    if not started:
        for exc in errors:
            print(str(exc))
            if hasattr(exc, 'response') and exc.response is not None:
                print('status: %d' % exc.response.status_code)
                print(exc.response.content)

        print('Could not start the daemon')
        try:
            out, err = daemon.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            out, err = 'Timeout', 'Timeout'

        if daemon.poll() is None:
            daemon.kill()

        print(err)
        print(out)

        if len(errors) > 0:
            raise errors[-1]
        else:
            raise Exception()

    return daemon


def start_docker():
    cmd = '%s -c "from loadsbroker.tests.fakedocker import main; main()"'
    cmd = cmd % sys.executable
    return _start_daemon(cmd, 7890)


def start_moto():
    cmd = '%s -c "from moto.server import main; main()" ec2'
    cmd = cmd % sys.executable
    return _start_daemon(cmd, 5000)


def start_influx():
    cmd = '%s -c "from loadsbroker.tests.fakeinflux import main; main()"'
    cmd = cmd % sys.executable
    return _start_daemon(cmd, 8086)


def start_broker():
    endpoints = os.path.join(os.path.dirname(__file__), 'endpoints.json')

    cmd = ('%s -c "from loadsbroker.main import main; main()" '
           '--aws-port 5000 --aws-endpoints %s '
           '--aws-skip-filters --aws-owner-id=')

    cmd = cmd % (sys.executable, endpoints)
    return _start_daemon(cmd, 8080)


def clear_boto_context():
    endpoints = os.environ.get('BOTO_ENDPOINTS')
    if endpoints is not None:
        del os.environ['BOTO_ENDPOINTS']
    s = StringIO()
    boto.config.write(s)
    s.seek(0)
    boto.config.clear()
    return s.read(), endpoints


def load_boto_context(config, endpoints=None):
    s = StringIO()
    s.write(config)
    boto.config.clear()
    boto.config.read(s)
    if endpoints is not None:
        os.environ['BOTO_ENDPOINTS'] = endpoints


def boto_cleared(func):
    @wraps(func)
    def _cleared(*args, **kwargs):
        context, endpoints = clear_boto_context()
        try:
            return func(*args, **kwargs)
        finally:
            load_boto_context(context, endpoints)
    return _cleared


def create_images():
    import logging
    logging.getLogger('boto').setLevel(logging.CRITICAL)

    # late import so BOTO_ENDPOINTS is seen
    from boto.ec2 import connect_to_region

    for region in AWS_REGIONS:
        conn = connect_to_region(
            region,
            aws_access_key_id='key',
            aws_secret_access_key='secret',
            port=5000, is_secure=False)

        reservation = conn.run_instances('ami-abcd1234')
        instance = reservation.instances[0]
        instance.modify_attribute("name", "CoreOS-stable")
        instance.modify_attribute("instanceType", "t1.micro")
        instance.modify_attribute("virtualization_type", "paravirtual")
        instance.modify_attribute("owner-id", "595879546273")
        conn.create_image(instance.id, "coreos-stable", "this is a test ami")


def start_all():
    # start docker
    docker = start_docker()

    # start moto
    moto = start_moto()

    # now that Moto runs, let's add a fake centos image there,
    # so our broker is happy
    create_images()

    # start influxdb
    influx = start_influx()

    # start the broker
    try:
        broker = start_broker()
    except Exception:
        docker.kill()
        moto.kill()
        influx.kill()
        raise

    return broker, moto, docker, influx


if __name__ == '__main__':
    print('Starting All daemons')
    daemons = start_all()
    print('Started')
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        for daemon in daemons:
            daemon.kill()
        print('Bye')
