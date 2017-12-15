# JMX Management Example

To run the example, simply type:
 
    mvn verify -Djavax.net.ssl.keyStore=target/server0/etc/activemq.example.keystore -Djavax.net.ssl.keyStorePassword=activemqexample -Djavax.net.ssl.trustStore=target/server0/etc/activemq.example.truststore -Djavax.net.ssl.trustStorePassword=activemqexample

from this directory, or add **-PnoServer** if you want to start and create the broker manually.

This example shows how to manage ActiveMQ Artemis using [JMX using SSL](http://java.sun.com/javase/technologies/core/mntr-mgmt/javamanagement/)

## Example configuration

ActiveMQ Artemis exposes its managed resources by default on the platform MBeanServer.

To access this MBeanServer remotely, add the following to the management.xml configuration:

    <connector connector-port="1099" connector-host="localhost"/>

With these properties, ActiveMQ Artemis broker will be manageable remotely using standard JMX URL on port `1099`.

## More information

*   [Java management guide](http://docs.oracle.com/javase/8/docs/technotes/guides/management/agent.html)