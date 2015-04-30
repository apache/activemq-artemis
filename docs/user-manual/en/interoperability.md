# Interoperability

## Stomp

[Stomp](http://stomp.github.com/) is a text-orientated wire protocol
that allows Stomp clients to communicate with Stomp Brokers. Apache ActiveMQ Artemis
now supports Stomp 1.0, 1.1 and 1.2.

Stomp clients are available for several languages and platforms making
it a good choice for interoperability.

## Native Stomp support

Apache ActiveMQ Artemis provides native support for Stomp. To be able to send and
receive Stomp messages, you must configure a `NettyAcceptor` with a
`protocols` parameter set to have `stomp`:

    <acceptor name="stomp-acceptor">tcp://localhost:61613?protocols=STOMP</acceptor>

With this configuration, Apache ActiveMQ Artemis will accept Stomp connections on the
port `61613` (which is the default port of the Stomp brokers).

See the `stomp` example which shows how to configure an Apache ActiveMQ Artemis server
with Stomp.

### Limitations

Message acknowledgements are not transactional. The ACK frame can not be
part of a transaction (it will be ignored if its `transaction` header is
set).

### Stomp 1.1/1.2 Notes

#### Virtual Hosting

Apache ActiveMQ Artemis currently doesn't support virtual hosting, which means the
'host' header in CONNECT fram will be ignored.

#### Heart-beating

Apache ActiveMQ Artemis specifies a minimum value for both client and server heart-beat
intervals. The minimum interval for both client and server heartbeats is
500 milliseconds. That means if a client sends a CONNECT frame with
heartbeat values lower than 500, the server will defaults the value to
500 milliseconds regardless the values of the 'heart-beat' header in the
frame.

### Mapping Stomp destinations to Apache ActiveMQ Artemis addresses and queues

Stomp clients deals with *destinations* when sending messages and
subscribing. Destination names are simply strings which are mapped to
some form of destination on the server - how the server translates these
is left to the server implementation.

In Apache ActiveMQ Artemis, these destinations are mapped to *addresses* and *queues*.
When a Stomp client sends a message (using a `SEND` frame), the
specified destination is mapped to an address. When a Stomp client
subscribes (or unsubscribes) for a destination (using a `SUBSCRIBE` or
`UNSUBSCRIBE` frame), the destination is mapped to an Apache ActiveMQ Artemis queue.

### STOMP and connection-ttl

Well behaved STOMP clients will always send a DISCONNECT frame before
closing their connections. In this case the server will clear up any
server side resources such as sessions and consumers synchronously.
However if STOMP clients exit without sending a DISCONNECT frame or if
they crash the server will have no way of knowing immediately whether
the client is still alive or not. STOMP connections therefore default to
a connection-ttl value of 1 minute (see chapter on
[connection-ttl](#connection-ttl) for more information. This value can
be overridden using connection-ttl-override.

If you need a specific connectionTtl for your stomp connections without
affecting the connectionTtlOverride setting, you can configure your
stomp acceptor with the "connectionTtl" property, which is used to set
the ttl for connections that are created from that acceptor. For
example:

    <acceptor name="stomp-acceptor">tcp://localhost:61613?protocols=STOMP;connectionTtl=20000</acceptor>

The above configuration will make sure that any stomp connection that is
created from that acceptor will have its connection-ttl set to 20
seconds.

> **Note**
>
> Please note that the STOMP protocol version 1.0 does not contain any
> heartbeat frame. It is therefore the user's responsibility to make
> sure data is sent within connection-ttl or the server will assume the
> client is dead and clean up server side resources. With `Stomp 1.1`
> users can use heart-beats to maintain the life cycle of stomp
> connections.

### Stomp and JMS interoperability

#### Using JMS destinations

As explained in [Mapping JMS Concepts to the Core API](jms-core-mapping.md),
JMS destinations are also mapped to Apache ActiveMQ Artemis
addresses and queues. If you want to use Stomp to send messages to JMS
destinations, the Stomp destinations must follow the same convention:

-   send or subscribe to a JMS *Queue* by prepending the queue name by
    `jms.queue.`.

    For example, to send a message to the `orders` JMS Queue, the Stomp
    client must send the frame:

        SEND
        destination:jms.queue.orders

        hello queue orders
        ^@

-   send or subscribe to a JMS *Topic* by prepending the topic name by
    `jms.topic.`.

    For example to subscribe to the `stocks` JMS Topic, the Stomp client
    must send the frame:

        SUBSCRIBE
        destination:jms.topic.stocks

        ^@

#### Sending and consuming Stomp message from JMS or Apache ActiveMQ Artemis Core API

Stomp is mainly a text-orientated protocol. To make it simpler to
interoperate with JMS and Apache ActiveMQ Artemis Core API, our Stomp implementation
checks for presence of the `content-length` header to decide how to map
a Stomp message to a JMS Message or a Core message.

If the Stomp message does *not* have a `content-length` header, it will
be mapped to a JMS *TextMessage* or a Core message with a *single
nullable SimpleString in the body buffer*.

Alternatively, if the Stomp message *has* a `content-length` header, it
will be mapped to a JMS *BytesMessage* or a Core message with a *byte[]
in the body buffer*.

The same logic applies when mapping a JMS message or a Core message to
Stomp. A Stomp client can check the presence of the `content-length`
header to determine the type of the message body (String or bytes).

#### Message IDs for Stomp messages

When receiving Stomp messages via a JMS consumer or a QueueBrowser, the
messages have no properties like JMSMessageID by default. However this
may bring some inconvenience to clients who wants an ID for their
purpose. Apache ActiveMQ Artemis Stomp provides a parameter to enable message ID on
each incoming Stomp message. If you want each Stomp message to have a
unique ID, just set the `stompEnableMessageId` to true. For example:

    <acceptor name="stomp-acceptor">tcp://localhost:61613?protocols=STOMP;stompEnableMessageId=true</acceptor>

When the server starts with the above setting, each stomp message sent
through this acceptor will have an extra property added. The property
key is `
            amq-message-id` and the value is a String representation of a
long type internal message id prefixed with "`STOMP`", like:

    amq-message-id : STOMP12345

If `stomp-enable-message-id` is not specified in the configuration,
default is `false`.

#### Handling of Large Messages with Stomp

Stomp clients may send very large bodys of frames which can exceed the
size of Apache ActiveMQ Artemis server's internal buffer, causing unexpected errors. To
prevent this situation from happening, Apache ActiveMQ Artemis provides a stomp
configuration attribute `stompMinLargeMessageSize`. This attribute
can be configured inside a stomp acceptor, as a parameter. For example:

       <acceptor name="stomp-acceptor">tcp://localhost:61613?protocols=STOMP;stompMinLargeMessageSize=10240</acceptor>

The type of this attribute is integer. When this attributed is
configured, Apache ActiveMQ Artemis server will check the size of the body of each
Stomp frame arrived from connections established with this acceptor. If
the size of the body is equal or greater than the value of
`stompMinLargeMessageSize`, the message will be persisted as a large
message. When a large message is delievered to a stomp consumer, the
HorentQ server will automatically handle the conversion from a large
message to a normal message, before sending it to the client.

If a large message is compressed, the server will uncompressed it before
sending it to stomp clients. The default value of
`stompMinLargeMessageSize` is the same as the default value of
[min-large-message-size](#large-messages.core.config).

### Stomp Over Web Sockets

Apache ActiveMQ Artemis also support Stomp over [Web
Sockets](http://dev.w3.org/html5/websockets/). Modern web browser which
support Web Sockets can send and receive Stomp messages from Apache ActiveMQ Artemis.

To enable Stomp over Web Sockets, you must configure a `NettyAcceptor`
with a `protocol` parameter set to `stomp_ws`:

    <acceptor name="stomp-ws-acceptor">tcp://localhost:61614?protocols=STOMP_WS</acceptor>

With this configuration, Apache ActiveMQ Artemis will accept Stomp connections over Web
Sockets on the port `61614` with the URL path `/stomp`. Web browser can
then connect to `ws://<server>:61614/stomp` using a Web Socket to send
and receive Stomp messages.

A companion JavaScript library to ease client-side development is
available from [GitHub](http://github.com/jmesnil/stomp-websocket)
(please see its [documentation](http://jmesnil.net/stomp-websocket/doc/)
for a complete description).

The `stomp-websockets` example shows how to configure Apache ActiveMQ Artemis server to
have web browsers and Java applications exchanges messages on a JMS
topic.

### StompConnect

[StompConnect](http://stomp.codehaus.org/StompConnect) is a server that
can act as a Stomp broker and proxy the Stomp protocol to the standard
JMS API. Consequently, using StompConnect it is possible to turn
Apache ActiveMQ Artemis into a Stomp Broker and use any of the available stomp clients.
These include clients written in C, C++, c\# and .net etc.

To run StompConnect first start the Apache ActiveMQ Artemis server and make sure that
it is using JNDI.

Stomp requires the file `jndi.properties` to be available on the
classpath. This should look something like:

    java.naming.factory.initial=org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory

Configure any required JNDI resources in this file according to the
documentation.

Make sure this file is in the classpath along with the StompConnect jar
and the Apache ActiveMQ Artemis jars and simply run `java org.codehaus.stomp.jms.Main`.

## REST

Please see [Rest Interface](rest.md)

## AMQP

Apache ActiveMQ Artemis supports the [AMQP
1.0](https://www.oasis-open.org/committees/tc_home.php?wg_abbrev=amqp)
specification. To enable AMQP you must configure a Netty Acceptor to
receive AMQP clients, like so:

    <acceptor name="stomp-acceptor">tcp://localhost:5672?protocols=AMQP</acceptor>


Apache ActiveMQ Artemis will then accept AMQP 1.0 clients on port 5672 which is the
default AMQP port.

There are 2 Stomp examples available see proton-j and proton-ruby which
use the qpid Java and Ruby clients respectively

### AMQP and security

The Apache ActiveMQ Artemis Server accepts AMQP SASL Authentication and will use this
to map onto the underlying session created for the connection so you can
use the normal Apache ActiveMQ Artemis security configuration.

### AMQP Links

An AMQP Link is a uni directional transport for messages between a
source and a target, i.e. a client and the Apache ActiveMQ Artemis Broker. A link will
have an endpoint of which there are 2 kinds, a Sender and A Receiver. At
the Broker a Sender will have its messages converted into an Apache ActiveMQ Artemis
Message and forwarded to its destination or target. A Receiver will map
onto an Apache ActiveMQ Artemis Server Consumer and convert Apache ActiveMQ Artemis messages back into
AMQP messages before being delivered.

### AMQP and destinations

If an AMQP Link is dynamic then a temporary queue will be created and
either the remote source or remote target address will be set to the
name of the temporary queue. If the Link is not dynamic then the the
address of the remote target or source will used for the queue. If this
does not exist then an exception will be sent

> **Note**
>
> For the next version we will add a flag to aut create durable queue
> but for now you will have to add them via the configuration

### AMQP and Coordinations - Handling Transactions

An AMQP links target can also be a Coordinator, the Coordinator is used
to handle transactions. If a coordinator is used the the underlying
HormetQ Server session will be transacted and will be either rolled back
or committed via the coordinator.

> **Note**
>
> AMQP allows the use of multiple transactions per session,
> `amqp:multi-txns-per-ssn`, however in this version Apache ActiveMQ Artemis will only
> support single transactions per session

## OpenWire

Apache ActiveMQ Artemis now supports the
[OpenWire](http://activemq.apache.org/openwire.html) protocol so that an
Apache ActiveMQ Artemis JMS client can talk directly to an Apache ActiveMQ Artemis server. To enable
OpenWire support you must configure a Netty Acceptor, like so:

    <acceptor name="openwire-acceptor">tcp://localhost:61616?protocols=OPENWIRE</acceptor>


The Apache ActiveMQ Artemis server will then listens on port 61616 for incoming
openwire commands. Please note the "protocols" is not mandatory here.
The openwire configuration conforms to Apache ActiveMQ Artemis's "Single Port" feature.
Please refer to [Configuring Single
Port](#configuring-transports.single-port) for details.

Please refer to the openwire example for more coding details.

Currently we support Apache ActiveMQ Artemis clients that using standard JMS APIs. In
the future we will get more supports for some advanced, Apache ActiveMQ Artemis
specific features into Apache ActiveMQ Artemis.
