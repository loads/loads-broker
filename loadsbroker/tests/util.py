import sys
import subprocess
import requests
import time
import os

from boto.ec2 import connect_to_region
from loadsbroker.aws import AWS_REGIONS


def start_docker():
    cmd = '%s -c "from loadsbroker.tests.fakedocker import main; main()"'
    cmd = cmd % sys.executable

    docker = subprocess.Popen(cmd, shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)

    # wait for the fake docker daemon to be ready
    starting = time.time()
    started = False

    errors = []

    while time.time() - starting < 2:
        try:
            requests.get('http://127.0.0.1:7890', timeout=.1)
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

        print('Could not start the fake docker!')
        try:
            out, err = docker.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            out, err = 'Timeout', 'Timeout'

        if docker.poll() is None:
            docker.kill()

        print(err)
        print(out)

        if len(errors) > 0:
            raise errors[-1]
        else:
            raise Exception()

    return docker


def start_moto():
    cmd = 'from moto.server import main; main()'
    cmd = '%s -c "%s" ec2' % (sys.executable, cmd)
    moto = subprocess.Popen(cmd, shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
    errors = []
    starting = time.time()
    started = False

    while time.time() - starting < 2.:
        try:
            requests.get('http://127.0.0.1:5000', timeout=.1)
            started = True
            break
        except Exception as exc:
            errors.append(exc)
            time.sleep(.1)

    if not started:
        print('Could not start Moto!')
        if len(errors) > 0:
            exc = errors[-1]
            print(str(exc))
            if hasattr(exc, 'response') and exc.response is not None:
                print('status: %d' % exc.response.status_code)
                print(exc.response.content)
        try:
            out, err = moto.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            out, err = 'Timeout', 'Timeout'

        if moto.poll() is None:
            moto.kill()

        print(err)
        print(out)

        if len(errors) > 0:
            raise errors[-1]
        else:
            raise Exception()

    return moto


def start_broker():
    endpoints = os.path.join(os.path.dirname(__file__), 'endpoints.json')

    cmd = ('%s -c "from loadsbroker.main import main; main()" '
           '--aws-port 5000 --aws-endpoints %s '
           '--aws-skip-filters --aws-owner-id=')

    cmd = cmd % (sys.executable, endpoints)

    broker = subprocess.Popen(cmd, shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE)

    # wait for the broker to be ready
    starting = time.time()
    started = False

    errors = []

    while time.time() - starting < 2:
        try:
            requests.get('http://127.0.0.1:8080', timeout=.1)
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

        print('Could not start the broker!')
        try:
            out, err = broker.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            out, err = 'Timeout', 'Timeout'

        if broker.poll() is None:
            broker.kill()

        print(err)
        print(out)

        if len(errors) > 0:
            raise errors[-1]
        else:
            raise Exception()

    return broker


# fake creds used for TRAVIS
_BOTO = """\
[Credentials]
aws_access_key_id = BFIAJI6H5WO5YDSELKAQ
aws_secret_access_key = p9hzfA6vPnKuMeTlZrGaYMe1P8880nXarcyJSQFA
"""

if 'TRAVIS' in os.environ:
    with open(os.path.join(os.path.expanduser('~'), '.boto'), 'w') as f:
        f.write(_BOTO)


def create_images():
    import logging
    logging.getLogger('boto').setLevel(logging.CRITICAL)
    endpoints = os.path.join(os.path.dirname(__file__), 'endpoints.json')
    os.environ['BOTO_ENDPOINTS'] = endpoints

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

    # start the broker
    try:
        broker = start_broker()
    except Exception:
        moto.kill()

    return broker, moto, docker


if __name__ == '__main__':
    broker, moto, docker = start_all()
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        broker.kill()
        moto.kill()
        docker.kill()
