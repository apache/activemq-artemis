# Protocols and Interoperability

## Protocols

ActiveMQ Artemis has a plugable protocol architecture.  Protocol plugins come in the form of ActiveMQ Artemis protocol
modules.  Each protocol module should be added to the brokers class path and are loaded by the broker at boot time.
ActiveMQ Artemis ships with 5 protocol modules out of the box.  The 5 modules offer support for the following protocols:

* AMQP
* OpenWire
* MQTT
* STOMP
* HornetQ

In addition to the protocols above ActiveMQ Artemis also offers support for it's own highly performant native protocol
 "Core".

## Configuring protocols

In order to make use of a particular protocol, a transport must be configured with the desired protocol enabled.  There
is a whole section on configuring transports that can be found [here](configuring-transports.md).

The default configuration shipped with the ActiveMQ Artemis distribution comes with a number of acceptors already
defined, one for each of the above protocols plus a generic acceptor that supports all protocols.  To enable a
protocol on a particular acceptor simply add a url parameter "protocol=AMQP,STOMP" to the acceptor url.  Where the value
of the parameter is a comma separated list of protocol names.  If the protocol parameter is omitted from the url all
protocols are enabled.

    <!-- The following example enables only MQTT on port 1883 -->
    <acceptors>
       <acceptor>tcp://localhost:1883?protocols=MQTT</acceptor>
    </acceptors>

    <!-- The following example enables MQTT and AMQP on port 61617 -->
    <acceptors>
       <acceptor>tcp://localhost:1883?protocols=MQTT,AMQP</acceptor>
    </acceptors>

    <!-- The following example enables all protocols on 61616 -->
    <acceptors>
       <acceptor>tcp://localhost:61616</acceptor>
    </acceptors>

## AMQP

Apache ActiveMQ Artemis supports the [AMQP
1.0](https://www.oasis-open.org/committees/tc_home.php?wg_abbrev=amqp)
specification. To enable AMQP you must configure a Netty Acceptor to
receive AMQP clients, like so:

    <acceptor name="amqp-acceptor">tcp://localhost:5672?protocols=AMQP</acceptor>


Apache ActiveMQ Artemis will then accept AMQP 1.0 clients on port 5672 which is the
default AMQP port.

There are 2 AMQP examples available see proton-j and proton-ruby which
use the qpid Java and Ruby clients respectively.

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

### AMQP and Multicast Queues (Topics)

Although amqp has no notion of topics it is still possible to treat amqp consumers or receivers as subscriptions rather
than just consumers on a queue. By default any receiving link that attaches to an address that has only multicast enabled
will be treated as a subscription and a subscription queue will be created. If the Terminus Durability is either UNSETTLED_STATE
or CONFIGURATION then the queue will be made durable, similar to a JMS durable subscription and given a name made up from
the container id and the link name, something like `my-container-id:my-link-name`. if the Terminus Durability is configured
as NONE then a volatile multicast queue will be created.

Artemis also supports the qpid-jms client and will respect its use of topics regardless of the prefix used for the address.

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

### AMQP scheduling message delivery

An AMQP message can provide scheduling information that controls the time in the future when the
message will be delivered at the earliest.  This information is provided by adding a message annotation
to the sent message.

There are two different message annotations that can be used to schedule a message for later delivery:

* `x-opt-delivery-time`
The specified value must be a positive long corresponding to the time the message should be made available
for delivery (in milliseconds).

* `x-opt-delivery-delay`
The specified value must be a positive long corresponding to the amount of milliseconds after the broker
receives the given message before it should be made available for delivery.

if both annotations are present in the same message then the broker will prefer the more specific `x-opt-delivery-time` value.

## OpenWire

Apache ActiveMQ Artemis now supports the
[OpenWire](http://activemq.apache.org/openwire.html) protocol so that an
Apache ActiveMQ 5.x JMS client can talk directly to an Apache ActiveMQ Artemis server. To enable
OpenWire support you must configure a Netty Acceptor, like so:

    <acceptor name="openwire-acceptor">tcp://localhost:61616?protocols=OPENWIRE</acceptor>


The Apache ActiveMQ Artemis server will then listens on port 61616 for incoming
openwire commands. Please note the "protocols" is not mandatory here.
The openwire configuration conforms to Apache ActiveMQ Artemis's "Single Port" feature.
Please refer to [Configuring Single Port](configuring-transports.md#single-port-support) for details.

Please refer to the openwire example for more coding details.

Currently we support Apache ActiveMQ Artemis clients that using standard JMS APIs. In
the future we will get more supports for some advanced, Apache ActiveMQ Artemis
specific features into Apache ActiveMQ Artemis.

### Connection Monitoring

OpenWire has a few parameters to control how each connection is monitored, they are:

* maxInactivityDuration:
It specifies the time (milliseconds) after which the connection is closed by the broker if no data was received.
Default value is 30000.

* maxInactivityDurationInitalDelay:
It specifies the maximum delay (milliseconds) before inactivity monitoring is started on the connection.
It can be useful if a broker is under load with many connections being created concurrently.
Default value is 10000.

* useInactivityMonitor:
A value of false disables the InactivityMonitor completely and connections will never time out.
By default it is enabled. On broker side you don't neet set this. Instead you can set the
connection-ttl to -1.

* useKeepAlive:
Whether or not to send a KeepAliveInfo on an idle connection to prevent it from timing out.
Enabled by default. Disabling the keep alive will still make connections time out if no data
was received on the connection for the specified amount of time.

Note at the beginning the InactivityMonitor negotiates the appropriate maxInactivityDuration and
maxInactivityDurationInitalDelay. The shortest duration is taken for the connection.

More details please see [ActiveMQ InactivityMonitor](http://activemq.apache.org/activemq-inactivitymonitor.html).

### Disable/Enable Advisories

By default, advisory topics ([ActiveMQ Advisory](http://activemq.apache.org/advisory-message.html))
are created in order to send certain type of advisory messages to listening clients. As a result,
advisory addresses and queues will be displayed on the management console, along with user deployed
addresses and queues. This sometimes cause confusion because the advisory objects are internally
managed without user being aware of them. In addition, users may not want the advisory topics at all
(they cause extra resources and performance penalty) and it is convenient to disable them at all
from the broker side.

The protocol provides two parameters to control advisory behaviors on the broker side.

* supportAdvisory
Whether or not the broker supports advisory messages. If the value is true, advisory addresses/
queues will be created. If the value is false, no advisory addresses/queues are created. Default
value is true. 

* suppressInternalManagementObjects
Whether or not the advisory addresses/queues, if any, will be registered to management service
(e.g. JMX registry). If set to true, no advisory addresses/queues will be registered. If set to
false, those are registered and will be displayed on the management console. Default value is
true.

The two parameters are configured on openwire acceptors, via URLs or API. For example:

    <acceptor name="artemis">tcp://127.0.0.1:61616?protocols=CORE,AMQP,OPENWIRE;supportAdvisory=true;suppressInternalManagementObjects=false</acceptor>

### Virtual Topic Consumer Destination Translation

For existing OpenWire consumers of virtual topic destinations it is possible to configure a mapping function
that will translate the virtual topic consumer destination into a FQQN address. This address then represents
the consumer as a multicast binding to an address representing the virtual topic. 

The configuration string property ```virtualTopicConsumerWildcards``` has two parts seperated by a ```;```. 
The first is the 5.x style destination filter that identifies the destination as belonging to a virtual topic.
The second identifies the number of ```paths``` that identify the consumer queue such that it can be parsed from the
destination.
For example, the default 5.x virtual topic with consumer prefix of ```Consumer.*.```, would require a
```virtualTopicConsumerWildcards``` filter of ```Consumer.*.>;2```. As a url parameter this transforms to ```Consumer.*.%3E%3B2``` when
the url significant characters ```>;``` are escaped with their hex code points. 
In an acceptor url it would be:

     <acceptor name="artemis">tcp://127.0.0.1:61616?protocols=OPENWIRE;virtualTopicConsumerWildcards=Consumer.*.%3E%3B2</acceptor>

This will translate ```Consumer.A.VirtualTopic.Orders``` into a FQQN of ```VirtualTopic.Orders::Consumer.A``` using the
int component ```2``` of the configuration to identify the consumer queue as the first two paths of the destination.
```virtualTopicConsumerWildcards``` is multi valued using a ```,``` separator. 
  
## MQTT

MQTT is a light weight, client to server, publish / subscribe messaging protocol.  MQTT has been specifically
designed to reduce transport overhead (and thus network traffic) and code footprint on client devices.
For this reason MQTT is ideally suited to constrained devices such as sensors and actuators and is quickly
becoming the defacto standard communication protocol for IoT.

Apache ActiveMQ Artemis supports MQTT v3.1.1 (and also the older v3.1 code message format).  To enable MQTT,
simply add an appropriate acceptor with the MQTT protocol enabled.  For example:

    <acceptor name="mqtt">tcp://localhost:1883?protocols=MQTT</acceptor>

By default the configuration shipped with Apache ActiveMQ Artemis has the above acceptor already defined, MQTT is
also active by default on the generic acceptor defined on port 61616 (where all protocols are enabled), in the out
of the box configuration.

The best source of information on the MQTT protocol is in the specification.  The MQTT v3.1.1 specification can
be downloaded from the OASIS website here: https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html

Some note worthy features of MQTT are explained below:

### MQTT Quality of Service

MQTT offers 3 quality of service levels.

Each message (or topic subscription) can define a quality of service that is associated with it.  The quality of service
level defined on a topic is the maximum level a client is willing to accept.  The quality of service level on a
message is the desired quality of service level for this message.  The broker will attempt to deliver messages to
subscribers at the highest quality of service level based on what is defined on the message and topic subscription.

Each quality of service level offers a level of guarantee by which a message is sent or received:

* QoS 0: AT MOST ONCE: Guarantees that a particular message is only ever received by the subscriber a maximum of one time.
This does mean that the message may never arrive.  The sender and the receiver will attempt to deliver the message,
but if something fails and the message does not reach it's destination (say due to a network connection) the message
may be lost.  This QoS has the least network traffic overhead and the least burden on the client and the broker and is often
useful for telemetry data where it doesn't matter if some of the data is lost.

* QoS 1: AT LEAST ONCE: Guarantees that a message will reach it's intended recipient one or more times.  The sender will
continue to send the message until it receives an acknowledgment from the recipient, confirming it has received the message.
The result of this QoS is that the recipient may receive the message multiple times, and also increases the network
overhead than QoS 0, (due to acks).  In addition more burden is placed on the sender as it needs to store the message
and retry should it fail to receive an ack in a reasonable time.

* QoS 2: EXACTLY ONCE: The most costly of the QoS (in terms of network traffic and burden on sender and receiver) this
QoS will ensure that the message is received by a recipient exactly one time.  This ensures that the receiver never gets
any duplicate copies of the message and will eventually get it, but at the extra cost of network overhead and complexity
required on the sender and receiver.

### MQTT Retain Messages

MQTT has an interesting feature in which messages can be "retained" for a particular address.  This means that once a
retain message has been sent to an address, any new subscribers to that address will receive the last sent retain
message before any others messages, this happens even if the retained message was sent before a client has connected
or subscribed.  An example of where this feature might be useful is in environments such as IoT where devices need to
quickly get the current state of a system when they are on boarded into a system.

### Will Messages

A will message can be sent when a client initially connects to a broker.  Clients are able to set a "will
message" as part of the connect packet.  If the client abnormally disconnects, say due to a device or network failure
the broker will proceed to publish the will message to the specified address (as defined also in the connect packet).
Other subscribers to the will topic will receive the will message and can react accordingly. This feature can be useful
 in an IoT style scenario to detect errors across a potentially large scale deployment of devices.

### Debug Logging

Detailed protocol logging (e.g. packets in/out) can be activated via the following steps:

1) Open `<ARTEMIS_INSTANCE>/etc/logging.properties` 
2) Add `org.apache.activemq.artemis.core.protocol.mqtt` to the `loggers` list.
3) Add this line to enable `TRACE` logging for this new logger: `logger.org.apache.activemq.artemis.core.protocol.mqtt.level=TRACE`
4) Ensure the `level` for the `handler` you want to log the message doesn't block the `TRACE` logging. For example,
   modify the `level` of the `CONSOLE` `handler` like so: `handler.CONSOLE.level=TRACE`

The MQTT specification doesn't dictate the format of the payloads which clients publish. As far as the broker is
concerned a payload is just just an array of bytes. However, to facilitate logging the broker will encode the payloads
as UTF-8 strings and print them up to 256 characters. Payload logging is limited to avoid filling the logs with potentially
hundreds of megabytes of unhelpful information.


### Wild card subscriptions

MQTT addresses are hierarchical much like a file system, and use "/" character to separate hierarchical levels.
Subscribers are able to subscribe to specific topics or to whole branches of a hierarchy.

To subscribe to branches of an address hierarchy a subscriber can use wild cards.

There are 2 types of wild card in MQTT:

 * "#" Multi level wild card.  Adding this wild card to an address would match all branches of the address hierarchy
 under a specified node.  For example:  /uk/#  Would match /uk/cities, /uk/cities/newcastle and also /uk/rivers/tyne.
 Subscribing to an address "#" would result in subscribing to all topics in the broker.  This can be useful, but should
 be done so with care since it has significant performance implications.

 * "+" Single level wild card.  Matches a single level in the address hierarchy.  For example /uk/+/stores would
   match /uk/newcastle/stores but not /uk/cities/newcastle/stores.


## Stomp

[Stomp](https://stomp.github.io/) is a text-orientated wire protocol
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

### Mapping Stomp destinations to addresses and queues

Stomp clients deals with *destinations* when sending messages and
subscribing. Destination names are simply strings which are mapped to
some form of destination on the server - how the server translates these
is left to the server implementation.

In Apache ActiveMQ Artemis, these destinations are mapped to *addresses* and *queues*
depending on the operation being done and the desired semantics (e.g. anycast or
multicast).

#### Sending

When a Stomp client sends a message (using a `SEND` frame), the protocol manager looks
at the message to determine where to route it and potentially how to create the address
and/or queue to which it is being sent. The protocol manager uses either of the following
bits of information from the frame to determine the routing type:

1. The value of the `destination-type` header. Valid values are `ANYCAST` and
`MULTICAST` (case sensitive).

2. The "prefix" on the `destination` header. See [additional info](address-model.md) on
prefixes.

If no indication of routing type is supplied then anycast semantics are used.

The `destination` header maps to an address of the same name. If the `destination` header
used a prefix then the prefix is stripped.

#### Subscribing

When a Stomp client subscribes to a destination (using a `SUBSCRIBE` frame), the protocol
manager looks at the frame to determine what subscription semantics to use and potentially how
to create the address and/or queue for the subscription. The protocol manager uses either of
the following bits of information from the frame to determine the routing type:

1. The value of the `subscription-type` header. Valid values are `ANYCAST` and
`MULTICAST` (case sensitive).

2. The "prefix" on the `destination` header. See [additional info](address-model.md) on
prefixes.

If no indication of routing type is supplied then anycast semantics are used.

The `destination` header maps to an address of the same name if multicast is used or to a queue
of the same name if anycast is used. If the `destination` header used a prefix then the prefix
is stripped.

### STOMP heart-beating and connection-ttl

Well behaved STOMP clients will always send a DISCONNECT frame before
closing their connections. In this case the server will clear up any
server side resources such as sessions and consumers synchronously.
However if STOMP clients exit without sending a DISCONNECT frame or if
they crash the server will have no way of knowing immediately whether
the client is still alive or not. STOMP connections therefore default to
a connection-ttl value of 1 minute (see chapter on
[connection-ttl](connection-ttl.md) for more information. This value can
be overridden using the `connection-ttl-override` property or if you
need a specific connectionTtl for your stomp connections without
affecting the broker-wide `connection-ttl-override` setting, you can
configure your stomp acceptor with the "connectionTtl" property, which
is used to set the ttl for connections that are created from that acceptor.
For example:

    <acceptor name="stomp-acceptor">tcp://localhost:61613?protocols=STOMP;connectionTtl=20000</acceptor>

The above configuration will make sure that any Stomp connection that is
created from that acceptor and does not include a `heart-beat` header
or disables client-to-server heart-beats by specifying a `0` value will
have its connection-ttl set to 20 seconds. The `connectionTtl` set on an
acceptor will take precedence over `connection-ttl-override`. The default
`connectionTtl` is 60,000 milliseconds.

Since Stomp 1.0 does not support heart-beating then all connections from
Stomp 1.0 clients will have a connection TTL imposed upon them by the broker
based on the aforementioned configuration options. Likewise, any Stomp 1.1
or 1.2 clients that don't specify a `heart-beat` header or disable client-to-server
heart-beating (e.g. by sending `0,X` in the `heart-beat` header) will have
a connection TTL imposed upon them by the broker.

For Stomp 1.1 and 1.2 clients which send a non-zero client-to-server `heart-beat`
header value then their connection TTL will be set accordingly. However, the broker
will not strictly set the connection TTL to the same value as the specified in the
`heart-beat` since even small network delays could then cause spurious disconnects.
Instead, the client-to-server value in the `heart-beat` will be multiplied by the
`heartBeatConnectionTtlModifer` specified on the acceptor. The
`heartBeatConnectionTtlModifer` is a decimal value that defaults to `2.0` so for
example, if a client sends a `heart-beat` header of `1000,0` the the connection TTL
will be set to `2000` so that the data or ping frames sent every 1000 milliseconds will
have a sufficient cushion so as not to be considered late and trigger a disconnect.
This is also in accordance with the Stomp 1.1 and 1.2 specifications which both state,
"because of timing inaccuracies, the receiver SHOULD be tolerant and take into account
an error margin."

The minimum and maximum connection TTL allowed can also be specified on the
acceptor via the `connectionTtlMin` and `connectionTtlMax` properties respectively.
The default `connectionTtlMin` is 1000 and the default `connectionTtlMax` is Java's
`Long.MAX_VALUE` meaning there essentially is no max connection TTL by default.
Keep in mind that the `heartBeatConnectionTtlModifer` is relevant here. For
example, if a client sends a `heart-beat` header of `20000,0` and the acceptor
is using a `connectionTtlMax` of `30000` and a default `heartBeatConnectionTtlModifer`
of `2.0` then the connection TTL would be `40000` (i.e. `20000` * `2.0`) which would
exceed the `connectionTtlMax`. In this case the server would respond to the client
with a `heart-beat` header of `0,15000` (i.e. `30000` / `2.0`). As described
previously, this is to make sure there is a sufficient cushion for the client
heart-beats in accordance with the Stomp 1.1 and 1.2 specifications. The same kind
of calculation is done for `connectionTtlMin`.

The minimum server-to-client heart-beat value is 500ms.

> **Note**
>
> Please note that the STOMP protocol version 1.0 does not contain any
> heart-beat frame. It is therefore the user's responsibility to make
> sure data is sent within connection-ttl or the server will assume the
> client is dead and clean up server side resources. With `Stomp 1.1`
> users can use heart-beats to maintain the life cycle of stomp
> connections.

### Selector/Filter expressions

Stomp subscribers can specify an expression used to select or filter
what the subscriber receives using the `selector` header. The filter
expression syntax follows the *core filter syntax* described in the
[Filter Expressions](filter-expressions.md) documentation.

### Stomp and JMS interoperability

#### Sending and consuming Stomp message from JMS or Apache ActiveMQ Artemis Core API

Stomp is mainly a text-orientated protocol. To make it simpler to
interoperate with JMS and Apache ActiveMQ Artemis Core API, our Stomp implementation
checks for presence of the `content-length` header to decide how to map
a Stomp 1.0 message to a JMS Message or a Core message.

If the Stomp 1.0 message does *not* have a `content-length` header, it will
be mapped to a JMS *TextMessage* or a Core message with a *single
nullable SimpleString in the body buffer*.

Alternatively, if the Stomp 1.0 message *has* a `content-length` header, it
will be mapped to a JMS *BytesMessage* or a Core message with a *byte[]
in the body buffer*.

The same logic applies when mapping a JMS message or a Core message to
Stomp. A Stomp 1.0 client can check the presence of the `content-length`
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

### Durable Subscriptions

The `SUBSCRIBE` and `UNSUBSCRIBE` frames can be augmented with special headers to create
and destroy durable subscriptions respectively.

To create a durable subscription the `client-id` header must be set on the `CONNECT` frame
and the `durable-subscription-name` must be set on the `SUBSCRIBE` frame. The combination
of these two headers will form the identity of the durable subscription.

To delete a durable subscription the `client-id` header must be set on the `CONNECT` frame
and the `durable-subscription-name` must be set on the `UNSUBSCRIBE` frame. The values for
these headers should match what was set on the `SUBSCRIBE` frame to delete the corresponding
durable subscription.

It is possible to pre-configure durable subscriptions since the Stomp implementation creates
the queue used for the durable subscription in a deterministic way (i.e. using the format of
`client-id`.`subscription-name`). For example, if you wanted to configure a durable
subscription on the address `myAddress` with a client-id of `myclientid` and a subscription
name of `mysubscription` then configure the durable subscription:

~~~
   <core xmlns="urn:activemq:core">
      ...
      <addresses>
         <address name="myAddress">
            <multicast>
               <queue name="myclientid.mysubscription"/>
            </multicast>
         </address>
      </addresses>
      ...
   </core>
~~~

### Handling of Large Messages with Stomp

Stomp clients may send very large frame bodies which can exceed the
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
broker will automatically handle the conversion from a large
message to a normal message, before sending it to the client.

If a large message is compressed, the server will uncompressed it before
sending it to stomp clients. The default value of
`stompMinLargeMessageSize` is the same as the default value of
[min-large-message-size](large-messages.md#configuring-parameters).

### Stomp Over Web Sockets

Apache ActiveMQ Artemis also support Stomp over [Web
Sockets](https://html.spec.whatwg.org/multipage/web-sockets.html). Modern web browser which
support Web Sockets can send and receive Stomp messages from Apache ActiveMQ Artemis.

Stomp over Web Sockets is supported via the normal Stomp acceptor:

    <acceptor name="stomp-ws-acceptor">tcp://localhost:61614?protocols=STOMP</acceptor>

With this configuration, Apache ActiveMQ Artemis will accept Stomp connections over Web
Sockets on the port `61614`. Web browser can
then connect to `ws://<server>:61614` using a Web Socket to send
and receive Stomp messages.

A companion JavaScript library to ease client-side development is
available from [GitHub](https://github.com/jmesnil/stomp-websocket)
(please see its [documentation](http://jmesnil.net/stomp-websocket/doc/)
for a complete description).

The payload length of websocket frames can vary between client implementations. By default
Apache ActiveMQ Artemis will accept frames with a payload length of 65,536. If the client
needs to send payloads longer than this in a single frame this length can be adjusted by
using the `stompMaxFramePayloadLength` URL parameter on the acceptor.

The `stomp-websockets` example shows how to configure Apache ActiveMQ Artemis server to
have web browsers and Java applications exchanges messages on a JMS
topic.

## REST

Please see [Rest Interface](rest.md)

