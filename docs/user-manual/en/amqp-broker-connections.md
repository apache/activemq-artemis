# Broker Connections

Instead of waiting for clients to connect, a broker can initiate a connection to another endpoint on a specific protocol.

Currently, this feature supports only the AMQP protocol. However, in the future, it might be expanded to other protocols.

You configure broker connections using a `<broker-connections>` XML element in the `broker.xml` configuration file.

```xml
<broker-connections>
    ...
</broker-connections>
```

# AMQP Server Connections

An ActiveMQ Artemis broker can initiate connections using the AMQP protocol. This means that the broker can connect to another AMQP server (not necessarily ActiveMQ Artemis) and create elements on that connection.

To define an AMQP broker connection, add an `<amqp-connection>` element within the `<broker-connections` element in the `broker.xml` configuration file. For example:

```xml
<broker-connections>
    <amqp-connection uri="tcp://MY_HOST:MY_PORT" name="my-broker" retry-interval="100" reconnect-attempts="-1" user="john" password="doe">
         ...
    </amqp-connection>
</broker-connections>
```

- `uri`: tcp://host:myport (this is a required argument)
- `name`: Name of the connection used for management purposes
- `user`: User name with which to connect to the endpoint (this is an optional argument)
- `password`: Password with which to connect to the endpoint (this is an optional argument)
- `retry-interval`: Time, in milliseconds to wait before retrying a connection after an error. The default value is `5000`.
- `reconnect-attempts`: default is -1 meaning infinite
- `auto-start` : Should the broker connection start automatically with the broker. Default is `true`. If false you need to call a management operation to start it.

*Notice*: If you disable auto-start on the broker connection, the start of the broker connection will only happen after the management method `startBrokerConnection(connectionName)` is called on the ServerController.

*Important*: The target endpoint needs permission for all operations that you configure. Therefore, If you are using a security manager, ensure that you perform the configured operations as a user with sufficient permissions.

# AMQP Server Connection Operations
The following types of operations are supported on a AMQP server connection:

* Senders
    * Messages received on specific queues are transferred to another endpoint
* Receivers
    * The broker pulls messages from another endpoint
* Peers
    * The broker creates both senders and receivers on another endpoint that knows how to handle them. Currently, this is implemented by Apache Qpid Dispatch.
* Mirrors
    * The broker uses an AMQP connection to another broker and duplicate messages and sends acknowledgements over the wire.

## Senders and Receivers
It is possible to connect an ActiveMQ Artemis broker to another AMQP endpoint simply by creating a sender or receiver broker connection element.

For a `sender`, the broker creates a message consumer on a queue that sends messages to another AMQP endpoint.

For a `receiver`, the broker creates a message producer on an address that receives messages from another AMQP endpoint.

Both elements work like a message bridge. However, there is no additional overhead required to process messages. Senders and receivers behave just like any other consumer or producer in ActiveMQ Artemis.

You can configure senders or receivers for specific queues. You can also match senders and receivers to specific addresses or _sets_ of addresses, using wildcard expressions. When configuring a sender or receiver, you can set the following properties:

- `match`: Match the sender or receiver to a specific address or __set__ of addresses, using a wildcard expression
- `queue-name`: Configure the sender or receiver for a specific queue


Some examples are shown below.

Using address expressions:
```xml
<broker-connections>
        <amqp-connection uri="tcp://MY_HOST:MY_PORT" name="my-broker">
                <sender match="queues.#"/>
                <!-- notice the local queues for remotequeues.# need to be created on this broker -->
                <receiver match="remotequeues.#"/>
        </amqp-connection>
</broker-connections>

<addresses>
        <address name="remotequeues.A">
                <anycast>
                        <queue name="remoteQueueA"/>
                </anycast>
        </address>
        <address name="queues.B">
                 <anycast>
                        <queue name="localQueueB"/>
                </anycast>
        </address>
</addresses>
```

Using queue names:
```xml
<broker-connections>
    <amqp-connection uri="tcp://MY_HOST:MY_PORT" name="my-broker">
        <receiver queue-name="remoteQueueA"/>
        <sender queue-name="localQueueB"/>
    </amqp-connection>
</broker-connections>

<addresses>
     <address name="remotequeues.A">
        <anycast>
           <queue name="remoteQueueA"/>
        </anycast>
     </address>
     <address name="queues.B">
        <anycast>
           <queue name="localQueueB"/>
        </anycast>
     </address>
</addresses>

```
*Important*: You can match a receiver only to a local queue that already exists. Therefore, if you are using receivers, make sure that you pre-create the queue locally. Otherwise, the broker cannot match the remote queues and addresses.

*Important*: Do not create a sender and a receiver to the same destination. This creates an infinite loop of sends and receives.


# Peers
A peer broker connection element is a combination of sender and receivers. The ActiveMQ Artemis broker creates both a sender and a receiver for a peer element, and the endpoint knows how to deal with the pair without creating an infinite loop of sending and receiving messages.

Currently, [Apache Qpid Dispatch Router](https://qpid.apache.org/components/dispatch-router/index.html) is a peer. ActiveMQ Artemis creates the pair of receivers and sender for each matching destination. These senders and receivers have special configuration to let Qpid Dispatch Router know to collaborate with ActiveMQ Artemis.

You can experiment with advanced networking scenarios with Qpid Dispatch Router and get a lot of benefit from the AMQP protocol and its ecosystem.

With a peer, you have the same properties that you have on a sender and receiver. For example:
```xml
<broker-connections>
    <amqp-connection uri="tcp://MY_HOST:MY_PORT" name="my-broker">
       <peer match="queues.#"/>
    </amqp-connection>
</broker-connections>

<addresses>
     <address name="queues.A">
        <anycast>
           <queue name="localQueueA"/>
        </anycast>
     </address>
     <address name="queues.B">
     <anycast>
        <queue name="localQueueB"/>
     </anycast>
    </address>
</addresses>
```

*Important:* Do not use this feature to connect to another broker, otherwise any message sent will be immediately ready to consume creating an infinite echo of sends and receives.

# Mirror 
The mirror option on the broker connection can capture events from the broker and pass them over the wire to another broker. This enables you to capture multiple asynchronous replicas. The following types of events are captured:

* Message routing
* Message acknowledgement
* Queue and address creation
* queue and address deletion

When you configure a mirror, these events are captured from the broker, stored on a local queue, and later forwarded to a target destination on another ActiveMQ Artemis broker.

To configure a mirror, you add a `<mirror>` element within the `<amqp-connection>` element.

The local queue is called `source-mirror-address`

You can specify the following optional arguments.

* `queue-removal`: Specifies whether a queue- or address-removal event is sent. The default value is `true`.
* `message-acknowledgements`: Specifies whether message acknowledgements are sent. The default value is `true`.
* `queue-creation`: Specifies whether a queue- or address-creation event is sent. The default value is `true`.
* `source-mirror-address`: By default, the mirror creates a non-durable temporary queue to store messages before they are sent to the other broker. If you define a name value for this property, an ANYCAST durable queue and address is created with the specified name.

An example of a mirror configuration is shown below:
```xml
<broker-connections>
    <amqp-connection uri="tcp://MY_HOST:MY_PORT" name="my-broker">
            <mirror  queue-removal="true" queue-creation="true" message-acknowledgements="true" source-mirror-address="myLocalSNFMirrorQueue"/>
    </amqp-connection>
</broker-connections>
```

## Catch up on Mirror
The broker will not send past events over the mirror. As the broker sends and receives messages, only a natural catch up would eventually happen.

## Disaster & Recovery considerations
As you use the mirror option to replicate data across datacenters, you have to take a few considerations:

* Currently we don't support quorums for activating the replica, so you have to manually control when your clients connect to the replica site.
* Make sure the replica site is passive. Having producers and consumers connected into both sites would be messy and could lead you to data integrity issues.
    * You can disable auto-start on the acceptor your clients use to connect, and only enable it after a disaster has occurred.
* Only the queues and addresses are mirrored. Consumer states will have to be reapplied on the replica when the clients reconnects (that applies to message groups, exclusive consumers or anything related to clients)
* Make sure your configuration options are copied over, including Diverts, security, last value queues, address settings and other configuration options.
* Have a way back route after a disaster.
    * You can have a disabled broker connection to be enabled after the disaster.


## Mirror example sending acknowledgements

On this example lets play with two brokers:
- sourceBroker
- replicaBroker

Add this configuration on sourceBroker:

```xml
<broker-connections>
    <amqp-connection uri="tcp://replicaBroker:6700" name="DRSite">
            <mirror message-acknowledgements="true"/>
    </amqp-connection>
</broker-connections>
```

On the replicaBroker, add disabled broker connection for failing back after a disaster, and also set the acceptors with autoStart=false

```xml

<acceptors>
     <!-- this one is for clients -->
     <acceptor name="artemis">tcp://0.0.0.0:61616?autoStart=false;tcpSendBufferSize=1048576;tcpReceiveBufferSize=1048576;amqpMinLargeMessageSize=102400;protocols=CORE,AMQP,STOMP,HORNETQ,MQTT,OPENWIRE;useEpoll=true;amqpCredits=1000;amqpLowCredits=300;amqpDuplicateDetection=true;autoStart=false</acceptor>
     <!-- this one is for DR communication -->
     <acceptor name="amqp">tcp://0.0.0.0:6700?autoStart=true;tcpSendBufferSize=1048576;tcpReceiveBufferSize=1048576;protocols=AMQP;useEpoll=true;amqpCredits=1000;amqpLowCredits=300;amqpMinLargeMessageSize=102400;amqpDuplicateDetection=true;autoStart=false</acceptor>
</acceptors>
<broker-connections>
    <amqp-connection uri="tcp://sourceBroker:6700" name="sourceBroker" auto-start="false">
            <mirror message-acknowledgements="true"/>
    </amqp-connection>
</broker-connections>
```

After a failure has occurred, you can use a management operation start on the acceptor:

- AccetorControl.start();

And you can call startBrokerConnection to enable the failback towards the live site:

- ActiveMQServerControl.startBrokerConnection("sourceBroker")

