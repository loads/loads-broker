.. _index:

========
Loads v2
========

`loads` v2 is an AWS-orchestrating load-generation tool, a.k.a load-tester.

Core features:

- Any language can be used to write a load-generator
- Metric collection (Statsd, logfiles) built-in
- Grafana metric dashboards for each run
- Ability to set up the service being load-tested
- Load-generation strategies can combine load-generators
- RESTful API for triggering load-tests

Narrative Documentation
=======================

To start learning about ``loads``, how to set it up, how to write load-
generators, and how to run load-testing strategies -- start here.

.. toctree::
   :maxdepth: 2

   narr/about


API Documentation
=================

``loads`` documentation for developers that wish to work directly with the
``loads`` code-base and/or create their own custom extensions for load-testing
orchestration.

.. toctree::
   :maxdepth: 1
   :glob:

   api/index
   api/*
   api/webapp/*
