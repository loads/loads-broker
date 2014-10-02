Docker image
============

Build the docker::

    $ docker build -t loads/loads-broker .

Run it::

    $ docker run -p 8083:8083 -p 8086:8086 --expose 8090 --expose 8099 loads/loads-broker

Add -d for running in the background.

