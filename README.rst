============
Loads-Broker
============

.. image:: https://travis-ci.org/loads/loads-broker.svg?branch=master
    :target: https://travis-ci.org/loads/loads-broker

Loadsv2 is split between two parts.

- The Loads Broker

  - Public API to run & check tests
  - Manages AWS Loads Tests Nodes
  - Manages information related to running of load-tests
  - Interacts with Docker Daemons

- The Loads Dashboard

  - Display ongoing load tests
  - Display info about the cluster
  - Manually run load-tests

The loads-broker project retains documentation regarding the organization of
the database and how the loads-broker functions.

`Complete Loads Broker documentation <http://loads-broker.readthedocs.io/>`_

Developing
==========

You will need Python 3.5 or later to develop with loads-broker. Once it is
on your path you can run the makefile:

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
