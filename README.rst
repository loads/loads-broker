============
Loads-Broker
============

.. note::

   This is a work in progress, and has no slick UX yet. **RECOMMENDED FOR DEVELOPERS ONLY.**

Implements:

- The Loads Broker

  - Public API to run & check tests
  - Manages AWS Loads Tests Nodes
  - Interacts with Docker Daemons

- The Loads Dashboard

  - Display ongoing load tests
  - Display info about the cluster


Installing
==========

To build the docker image, you can run from the directory:

.. code-block:: bash

    $ docker build -t loads/loads-broker


To run the broker you will need Docker, and can install the container with:

.. code-block:: bash

    $ docker pull loads/loads-broker

.. note::

    Docker container unavailable at this stage of development.


Developing
==========

You will need Python 3.4 or later to develop with loads-broker. Once it is
on your path you can run the makefile::

.. code-block:: bash

    > make


Running in Develop Mode
=======================

Ensure that you have a ~/.boto file or other boto capable config file in
place. For example, a ~/.boto file should look like this:

.. code-block:: txt

    [Credentials]
    aws_access_key_id = YOURACCESSKEY
    aws_secret_access_key = YOURSECRETKEY
