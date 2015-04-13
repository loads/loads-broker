Project Files
*************

Loads uses project files to populate its database on startup. A service under
test can define its own ``project.json`` file containing test plans and steps
specific to that service.

An overview of the database schema is available on `Read the Docs
<http://loadsv2.readthedocs.org/en/latest/narr/about.html#database-schema>`_.
Each database table corresponds to a section in the project file:

.. code-block:: js

    {
      "name": "Project name",
      "home_page": "https://github.com/mozilla-services/my-project",
      "plans": [{
        "name": "Plan name",
        "steps": [{
          "name": "Step 1 name"
          // Step settings...
        }, {
          "name": "Step 2 name"
          // Step settings...
        }
        /* Steps... */]
      }
      /* Plans... */]
    }

Plans
=====

A test **plan** describes a particular test configuration for a project. Each
plan comprises a list of steps; executing a plan allocates EC2 instances for
each step.

Plans vary depending on the tester application and the desired load pattern: a
simple plan might specify a single step that makes requests via ``curl`` or
``wget`` in a loop; a more typical plan could stagger multiple testers and
vary the number of requests per step.

If the service under test is Dockerized and reads `configuration settings
<http://12factor.net/config>`_ from environment variables, a separate step
could start an ad-hoc test cluster. This makes it possible to adjust resource
limits and other parameters in response to test feedback, without redeploying
to a shared staging environment.

A plan contains the following properties:

* ``name`` (String): The plan name.
* ``description`` (String): A human-readable description of this plan.
* ``steps`` (Array): A list of steps executed as part of this plan.

Steps
=====

**Steps** are executed as part of a test plan, and correspond to Docker
containers running on EC2 instances. Containers are lightweight environments
that run individual applications in isolation, but without the overhead of a
virtual machine.

When executing a step, Loads creates containers for `Heka
<https://hekad.readthedocs.org/>`_ and `dnsmasq
<http://www.thekelleys.org.uk/dnsmasq/doc.html>`_, records the start and stop
time, and launches the step container with a set of environment variables and
command-line arguments. Heka sends CPU, memory, and statsd metrics collected
from running instances to a central InfluxDB node for analysis.

A step contains the following properties:

* ``name`` (String): The step name.
* ``instance_count`` (Integer, optional): The desired number of instances for
  this step. Defaults to 1 instance.
* ``instance_region`` (String, optional): The `EC2 region
  <http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/using-regions-availability-zones.html>`_
  in which to start the instances. Defaults to ``"us-west-2"``.
* ``instance_type`` (String, optional): The `EC2 instance type
  <https://aws.amazon.com/ec2/instance-types/>`_. Defaults to ``"t1.micro"``.
* ``node_delay`` (Seconds, optional): The time to wait before creating each
  instance in this step. Defaults to 0.
* ``run_delay`` (Seconds, optional): The time to wait before running this
  step, once all instances have been created. Defaults to 0; i.e., runs
  immediately.
* ``run_max_time`` (Seconds, optional): The running time of this step, once all
  instances have been created. Defaults to 600 seconds.
* ``container_name`` (String): The Docker image name and tag, e.g.,
  ``"bbangert/pushgo:1.5rc1"``.
* ``container_url`` (URL, optional): A URL to a tarball containing the Docker
  image. If specified, Loads will download the image from this URL instead of
  the Docker Hub.
* ``environment_data`` (Array or newline-separated string, optional):
  Environment variables to use for this container. Subject to interpolation.
* ``additional_command_args`` (String, optional): Additional arguments to pass
  to the container ``ENTRYPOINT``, or the full command name and arguments if
  the container's ``Dockerfile`` does not specify an entry point. Subject to
  interpolation.
* ``dns_name`` (String, optional): A round-robin DNS name for all instances in
  this step. For example, if an application in a step starts an HTTP server on
  port 8000, setting the ``dns_name`` to ``test.mozilla.dev`` allows testers
  in subsequent steps to make requests to ``http://test.mozilla.dev:8000``.
  This is useful for creating test clusters.
* ``port_mapping`` (Comma-separated string, optional): A mapping of container
  ports to host ports, in the form of ``container:host``.
* ``volume_mapping`` (Comma-separated string, optional): A mapping of container
  volumes to port volumes, in the form of ``container:host:mode``. ``mode`` is
  optional and defaults to read-only; if set to ``rw``, the ``host`` path will
  be mounted as read-write. Paths are subject to interpolation.
* ``docker_series`` (String, optional): The InfluxDB time series containing CPU
  and memory stats for this step. Defaults to ``"stats".``
* ``prune_running`` (Boolean, optional): Whether unresponsive running instances
  should be terminated. Defaults to ``true``.

Interpolation
=============

In addition to the variables specified in the ``environment_data`` field, Loads
exposes the following variables to each container:

* ``HOST_IP``: The external EC2 IP address.
* ``PRIVATE_IP``: The private IP address of the EC2 instance.
* ``STATSD_HOST``: The statsd host name for metrics.
* ``STATSD_PORT``: The stasd port.

These variables may also be referenced in all fields that support
interpolation. For example, to capture container logs for each test run:

.. code-block:: json

    {
      "volume_mapping": "/var/log:/var/log/$RUN_ID:rw"
    }
