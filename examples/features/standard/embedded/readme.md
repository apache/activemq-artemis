# Embedded Broker with Programmatic Config Example

To run the example, simply type **mvn verify** from this directory.

This examples shows how to setup and run an embedded broker using ActiveMQ Artemis.

ActiveMQ Artemis was designed using POJOs (Plain Old Java Objects) which means embedding ActiveMQ Artemis in your own application is as simple as instantiating a few objects.

This example does not use any configuration files. The broker is configured using POJOs and can be easily ported to any dependency injection framework.