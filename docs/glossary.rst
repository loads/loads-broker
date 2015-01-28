.. _glossary:

Glossary
========

.. glossary::
   :sorted:

   AWS
   	 Amazon Web Services.

   	 .. seealso::

   	 	`AWS documentation <http://aws.amazon.com/>`_

   load generator
     A load-generator is a program that should generate load. The
     load-generator runs on many machines and should feed relevant metrics
     data to the local :term:`statsd` receiver. The only requirement on the
     load-generator is that it should be configurable solely via environment
     and/or command line arguments. Load-generators are packaged for use in
     ``loads`` via :term:`docker` as containers.

   loads-broker
     load-testing orchestration program, written in Python 3.

   statsd
     A network daemon that collects stats metrics. In ``loads`` this is
     usually a local :term:`heka` instance.

     .. seealso::

     	`Etsy's statsd <https://github.com/etsy/statsd>`_

   heka
     A log/metric analysis daemon, that is run on every machine the
     load-generation strategy utilizes.

     .. seealso::

     	`heka documentation <http://hekad.readthedocs.org/>`_

   docker
     Docker makes it convenient to package programs up in easily runnable and
     distributable containers.

     .. seealso::

     	`docker website <http://docker.io/>`_

   dockerized
     The packaging of a program to run inside a :term:`docker` container.
