# Artemis to 5.x JMS Bridge

This is an example of using the JMS bridge shipped with the Artemis broker to bridge to a 5.x broker.

Notes:
 
- The Artemis JMS bridge is a general purpose bridge and can be used to bridge to any JMS provider which implements JNDI.
This example however is just focusing on integration with 5.x.
- The Artemis JMS bridge can "push" _and_ "pull" messages so it can be used to move messages both ways.

##Prerequisites

- install ActiveMQ 5.x
- install ActiveMQ Artemis

##Preparing

1) From the root dir run `mvn clean package`.

2) Copy artemis-jms-bridge/target/artemis-jms-bridge-<version>.war to the web directory of the Artemis installation.

3) Create an instance of the Artemis broker `$ARTEMIS_HOME/bin/artemis create --allow-anonymous myBroker`

4) Edit the `$ARTEMIS_INSTANCE/etc/broker.xml` and change the `artemis` acceptor to run on 61617 and add the `invm` acceptor.

```xml
<acceptors>
   <acceptor name="artemis">tcp://0.0.0.0:61617?tcpSendBufferSize=1048576;tcpReceiveBufferSize=1048576;protocols=CORE,AMQP,STOMP,HORNETQ,MQTT,OPENWIRE;useEpoll=true;amqpCredits=1000;amqpLowCredits=300</acceptor>
   <acceptor name="invm">vm://0</acceptor>
</acceptors>
```

5) Edit `$ARTEMIS_INSTANCE/etc/bootstrap.xml` and add the war file.

```xml
<app url="bridge" war="artemis-jms-bridge-<version>.war"/>
```

##Testing

Start the ActiveMQ broker.

`$ACTIVEMQ_HOME/bin/standalone`

Start the Artemis broker.

`$ARTEMIS_INSTANCE/bin/artemis run`

Send some messages to the queue TEST.BAR via the Artemis console.

`$ARTEMIS_INSTANCE/bin/artemis producer --destination queue://TEST.BAR --url tcp://localhost:61617 --message-count 1`

Log into the ActiveMQ 5.x console and browse the messages in the TEST.BAR queue.

# 5.x to Artemis Camel JMS Bridge

This is an alternative to using the Artemis JMS bridge using Camel in the 5.x broker to bridge messages to Artemis. 
There isn't anything to deploy here. It's just a set of instructions.

This approach might be preferred if for example you only have access to the 5.x broker.

## Prerequisites

- install ActiveMQ 5.x
- install ActiveMQ Artemis

## Preparing

1) Copy `lib/client/artemis-jms-client-all.jar` to the `$5X_HOME/lib` directory.

2) Add the bridge configuration to `activemq.xml`:

```xml
   <bean id="5xConnectionFactory" class="org.apache.activemq.ActiveMQConnectionFactory">
      <property name="brokerURL" value="tcp://localhost:61616"/>
      <property name="userName" value="admin"/>
      <property name="password" value="password"/>
   </bean>

   <bean id="jmsConfig" class="org.apache.camel.component.jms.JmsConfiguration">
      <property name="connectionFactory" ref="5xConnectionFactory"/>
      <property name="concurrentConsumers" value="10"/>
   </bean>

   <bean id="activemq" class="org.apache.activemq.camel.component.ActiveMQComponent">
      <property name="configuration" ref="jmsConfig"/>
   </bean>

   <bean id="artemisConnectionFactory" class="org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory">
      <constructor-arg name="url" value="tcp://localhost:61617"/>
   </bean>

   <bean id="artemisConfig" class="org.apache.camel.component.jms.JmsConfiguration">
      <property name="connectionFactory" ref="artemisConnectionFactory"/>
      <property name="concurrentConsumers" value="10"/>
   </bean>

   <bean id="artemis" class="org.apache.camel.component.jms.JmsComponent">
      <property name="configuration" ref="artemisConfig"/>
   </bean>

   <camelContext id="bridgeContext" trace="false" xmlns="http://camel.apache.org/schema/spring">
      <route id="bridge_TEST.FOO">
         <from uri="activemq:queue:TEST.FOO"/>
         <to uri="artemis:queue:TEST.FOO"/>
      </route>
   </camelContext>
```

3) Ensure the `xsi:schemalocation` in activemq.xml contains the necessary Camel schemas:

```
http://camel.apache.org/schema/spring http://camel.apache.org/schema/spring/camel-spring.xsd
```

4) Create an instance of the Artemis broker `$ARTEMIS_HOME/bin/artemis create --allow-anonymous myBroker`

5) Edit the `$ARTEMIS_INSTANCE/etc/broker.xml` and change the acceptor to listen to port 61617. Comment or remove all other acceptors.

```xml
<acceptors>
   <acceptor name="artemis">tcp://0.0.0.0:61617?tcpSendBufferSize=1048576;tcpReceiveBufferSize=1048576;protocols=CORE,AMQP,STOMP,HORNETQ,MQTT,OPENWIRE;useEpoll=true;amqpCredits=1000;amqpLowCredits=300</acceptor>
</acceptors>
```

6) Edit `$ARTEMIS_INSTANCE/etc/bootstrap.xml` so that the embedded web broker runs on a different port that the 5.x broker (e.g. 8162):

```xml
<web bind="http://localhost:8162" path="web">
```

## Testing

Start the Artemis broker.

`$ARTEMIS_INSTANCE/bin/artemis run`

Start the ActiveMQ 5.x broker.

`$5X_HOME/bin/activemq start`

Send some messages to the ActiveMQ 5.x broker.

`$5X_HOME/bin/activemq producer --user admin --password password --destination queue://TEST.FOO`

Log into the ActiveMQ Artemis console and browse the messages in the `TEST.FOO` queue.

