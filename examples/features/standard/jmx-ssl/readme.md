# JMX Management Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows how to manage ActiveMQ Artemis using [JMX over SSL](http://www.oracle.com/technetwork/java/javase/tech/javamanagement-140525.html)

## Example configuration

ActiveMQ Artemis exposes its managed resources by default on the platform MBeanServer.

To access this MBeanServer remotely, add the following to the management.xml configuration:

    <connector connector-port="1099" connector-host="localhost"/>

With these properties, ActiveMQ Artemis broker will be manageable remotely using standard JMX URL on port `1099`.

## More information

*   [Java management guide](https://docs.oracle.com/javase/8/docs/technotes/guides/management/agent.html)