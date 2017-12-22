# Client Kickoff Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows how to kick off a client connected to ActiveMQ using [JMX](http://www.oracle.com/technetwork/java/javase/tech/javamanagement-140525.html)

The example will connect to ActiveMQ Artemis. Using JMX, we will list the remote addresses connected to the broker and close the corresponding connections. The client will be kicked off from ActiveMQ Artemis receiving an exception that its JMS connection was interrupted.

## Example configuration

ActiveMQ Artemis exposes its managed resources by default on the platform MBeanServer.

To access this MBeanServer remotely, the Java Virtual machine must be started with system properties:

    -Dcom.sun.management.jmxremote
    -Dcom.sun.management.jmxremote.port=3000
    -Dcom.sun.management.jmxremote.ssl=false
    -Dcom.sun.management.jmxremote.authenticate=false

These properties are explained in the Java [management guide](https://docs.oracle.com/javase/8/docs/technotes/guides/management/agent.html#gdenl) (please note that for this example, we will disable user authentication for simplicity).

With these properties, ActiveMQ Artemis broker will be manageable remotely using standard JMX URL on port `3000`.