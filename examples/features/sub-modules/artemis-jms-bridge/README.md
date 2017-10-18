# artemis-jms-bridge

An example project showing how to do different varieties of bridging with ActiveMQ Brokers.

## ActiveMQ to Artemis Camel Bridge

This is an example of using Camel in the ActiveMQ broker to bridge messages between ActiveMQ and Artemis.

### Prerequisites

- install ActiveMQ
- install Artemis

### Preparing

From the root directory run `mvn clean package`

Copy activemq-artemis-camel/target/activemq-artemis-camel-1.0.0-SNAPSHOT.war to the deploy dir of the ActiveMQ installation.

Create an instance of the Artemis broker `$ARTEMIS_HOME/bin/artemis create --allow-anonymous --user admin --password password  myBroker`

Edit the $ARTEMIS_INSTANCE/etc/broker.xml and change the acceptor to listen to port 61617. Comment or remove all other acceptors.

```xml
<acceptors>
   <acceptor name="artemis">tcp://0.0.0.0:61617?tcpSendBufferSize=1048576;tcpReceiveBufferSize=1048576;protocols=CORE,AMQP,STOMP,HORNETQ,MQTT,OPENWIRE;useEpoll=true;amqpCredits=1000;amqpLowCredits=300</acceptor>
</acceptors>
```

### Testing

Start the Artemis broker.

`$ARTEMIS_INSTANCE/bin/artemis run`

Start the ActiveMQ broker.

`$ACTIVEMQ_HOME/bin/standalone`

Send some messages to the ActiveMQ broker.

`./apache-activemq-5.11.0/bin/activemq producer --user admin --password password --destination queue://TEST.FOO`

Log into the Artemis console and browse the messages in the TEST.FOO queue.

## Artemis to ActiveMQ JMS Bridge

This is an example of using the JMS bridge shipped with the Artemis broker to bridge to ActiveMQ.

###Prerequisites

- install ActiveMQ
- install Artemis

###Preparing

From the root dir run `mvn clean package`.

Copy artemis-jms-bridge/target/artemis-jms-bridge-1.0.0-SNAPSHOT.war to the web directory of the Artemis installation.

Create an instance of the Artemis broker `$ARTEMIS_HOME/bin/artemis create --allow-anonymous --user admin --password password  myBroker`

Edit the $ARTEMIS_INSTANCE/etc/broker.xml and change the acceptor to use invm.

```xml
<acceptors>
   <acceptor name="artemis">tcp://0.0.0.0:61617?tcpSendBufferSize=1048576;tcpReceiveBufferSize=1048576;protocols=CORE,AMQP,STOMP,HORNETQ,MQTT,OPENWIRE;useEpoll=true;amqpCredits=1000;amqpLowCredits=300</acceptor>
   <acceptor name="invm">vm://0</acceptor>
</acceptors>
```

Edit the $ARTEMIS_INSTANCE/etc/bootstrap.xml and add the war file.

```xml
<app url="bridge" war="artemis-jms-bridge-1.0.0-SNAPSHOT.war"/>
```

###Testing


Start the ActiveMQ broker.

`$ACTIVEMQ_HOME/bin/standalone`

Start the Artemis broker.

`$ARTEMIS_INSTANCE/bin/artemis run`

Send some messages to the queue TEST.BAR via the Artemis console.

`$ARTEMIS_INSTANCE/bin/artemis producer --user admin --password password --destination queue://TEST.BAR --url tcp://localhost:61617 --message-count 1`

Log into the ActiveMQ console and browse the messages in the TEST.BAR queue.
