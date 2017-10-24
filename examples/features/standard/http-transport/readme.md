# JMS HTTP Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to configure ActiveMQ Artemis to use the HTTP protocol as its transport layer.

ActiveMQ Artemis supports a variety of network protocols to be its underlying transport without any specific code change.

This example is taken from the queue example without any code change. By changing the client's URL in `jndi.properties` one can get ActiveMQ Artemis working with the HTTP transport.