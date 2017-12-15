# JMS Queue Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to send and receive a message to a JMS Queue using ActiveMQ Artemis.

Queues are a standard part of JMS, please consult the JMS 1.1 specification for full details.

A Queue is used to send messages point to point, from a producer to a consumer. The queue guarantees message ordering between these 2 points.

Notice this example is using pretty much a default stock configuration