# Apache Tomcat Support


## Resource Context Client Configuration

Apache ActiveMQ Artemis provides support for configuring the client, in the tomcat resource context.xml of Tomcat container.

This is very similar to the way this is done in ActiveMQ 5.x so anyone migrating should find this familiar.
Please note though the connection url and properties that can be set for ActiveMQ Artemis are different please see [Migration Documentation](https://activemq.apache.org/artemis/migration/)

### Example of Connection Factory
```xml
<Context>
    ...
  <Resource name="jms/ConnectionFactory" auth="Container" type="org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory" description="JMS Connection Factory"
        factory="org.apache.activemq.artemis.jndi.JNDIReferenceFactory" brokerURL="tcp://localhost:61616" />
    ...
</Context>
````

### Example of Destination (Queue and Topic)

```xml
<Context>
  ...
  <Resource name="jms/ExampleQueue" auth="Container" type="org.apache.activemq.artemis.jms.client.ActiveMQQueue" description="JMS Queue"
        factory="org.apache.activemq.artemis.jndi.JNDIReferenceFactory" address="ExampleQueue" />
  ...
  <Resource name="jms/ExampleTopic" auth="Container" type="org.apache.activemq.artemis.jms.client.ActiveMQTopic" description="JMS Topic"
         factory="org.apache.activemq.artemis.jndi.JNDIReferenceFactory" address="ExampleTopic" />
  ...
</Context>
````

## Example Tomcat App

A sample Tomcat app with the container context configured as an example can be seen here: 

/examples/features/sub-modules/tomcat