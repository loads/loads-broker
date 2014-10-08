Docker image
============

Build the docker::

    $ docker build -t loads/loads-broker .


Run it::

    $ docker run -p 8083:8083 -p 8086:8086 --expose 8090 --expose 8099 \
        -e AWS_ACCESS_KEY_ID=xxx -e AWS_SECRET_ACCESS_KEY=xxx loads/loads-broker

Add -d for running in the background.

