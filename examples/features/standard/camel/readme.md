# Camel Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example contains 2 different Maven modules:

1) `camel-broker` The module responsible for creating the broker, deploying the WAR-based Camel application, and running the client.
2) `camel-war` The module used to build the WAR-based Camel application.

The overall goal of this example is to demonstrate how to build and deploy a Camel route to the broker.

The client itself is essentially the same as the one in the `core-bridge` example except there is only 1 broker in this
example rather than 2. A Camel route defined in the WAR is responsible for moving messages between 2 queues. The client
sends a message to one queue, the Camel route moves that message to a second queue, and then the client reads that
message from the second queue.