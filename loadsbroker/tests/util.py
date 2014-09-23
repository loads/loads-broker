import sys
import subprocess
import requests
import time
import os


def start_moto():
    cmd = 'from moto.server import main; main()'
    cmd = '%s -c "%s" ec2' % (sys.executable, cmd)
    return subprocess.Popen(cmd, shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)


def start_broker():
    endpoints = os.path.join(os.path.dirname(__file__), 'endpoints.json')
    cmd = 'from loadsbroker.main import main; main()'
    cmd = '%s -c "%s" --aws-port 5000 --aws-endpoints %s' % (
        sys.executable,
        cmd, endpoints)

    return subprocess.Popen(cmd, shell=True)
    #,
    #                        stdout=subprocess.PIPE,
    #                        stderr=subprocess.PIPE)



# fake creds used for TRAVIS
_BOTO = """\
[Credentials]
aws_access_key_id = BFIAJI6H5WO5YDSELKAQ
aws_secret_access_key = p9hzfA6vPnKuMeTlZrGaYMe1P8880nXarcyJSQFA
"""


def start_all():
    if 'TRAVIS' in os.environ:
        with open(os.path.join(os.path.expanduser('~'), '.boto'), 'w') as f:
            f.write(_BOTO)

    errors = []
    # start moto
    moto = start_moto()
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
        print('Could not start the moto!')
        if len(errors) > 0:
            exc = errors[-1]
            print(str(exc))
            if hasattr(exc, 'response') and exc.response is not None:
                print('status: %d' % exc.response.status_code)
                print(exc.response.content)
        try:
            out, err = moto.communicate(timeout=10)
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

    # start the broker
    broker = start_broker()

    # wait for the broker to be ready
    starting = time.time()
    started = False

    while time.time() - starting < 1:
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
            out, err = broker.communicate(timeout=10)
        except subprocess.TimeoutExpired:
            out, err = 'Timeout', 'Timeout'

        if broker.poll() is None:
            broker.kill()

        if moto.poll() is None:
            moto.kill()

        print(err)
        print(out)
        moto.kill()

        if len(errors) > 0:
            raise errors[-1]
        else:
            raise Exception()

    return broker, moto


if __name__ == '__main__':
    broker, moto = start_all()
    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        broker.kill()
        moto.kill()
