# STOMP

[STOMP](https://stomp.github.io/) is a text-orientated wire protocol that
allows STOMP clients to communicate with STOMP Brokers. Apache ActiveMQ Artemis
supports STOMP 1.0, 1.1 and 1.2.

STOMP clients are available for several languages and platforms making it a
good choice for interoperability.

By default there are `acceptor` elements configured to accept STOMP connections
on ports `61616` and `61613`.

See the general [Protocols and Interoperability](protocols-interoperability.md)
chapter for details on configuring an `acceptor` for STOMP.

Refer to the STOMP examples for a look at some of this functionality in action.

## Limitations

The STOMP specification identifies **transactional acknowledgements** as an
optional feature. Support for transactional acknowledgements is not implemented
in Apache ActiveMQ Artemis. The `ACK` frame can not be part of a transaction.
It will be ignored if its `transaction` header is set.

## Virtual Hosting

Apache ActiveMQ Artemis currently doesn't support virtual hosting, which means
the `host` header in `CONNECT` frame will be ignored.

## Mapping STOMP destinations to addresses and queues

STOMP clients deals with *destinations* when sending messages and subscribing.
Destination names are simply strings which are mapped to some form of
destination on the server - how the server translates these is left to the
server implementation.

In Apache ActiveMQ Artemis, these destinations are mapped to *addresses* and
*queues* depending on the operation being done and the desired semantics (e.g.
anycast or multicast).

## Sending

When a STOMP client sends a message (using a `SEND` frame), the protocol
manager looks at the message to determine where to route it and potentially how
to create the address and/or queue to which it is being sent. The protocol
manager uses either of the following bits of information from the frame to
determine the routing type:

1. The value of the `destination-type` header. Valid values are `ANYCAST` and
   `MULTICAST` (case sensitive).

2. The "prefix" on the `destination` header. See [additional
   info](address-model.md#using-prefixes-to-determine-routing-type) on
   prefixes.

If no indication of routing type is supplied then anycast semantics are used.

The `destination` header maps to an address of the same name. If the
`destination` header used a prefix then the prefix is stripped.

## Subscribing

When a STOMP client subscribes to a destination (using a `SUBSCRIBE` frame),
the protocol manager looks at the frame to determine what subscription
semantics to use and potentially how to create the address and/or queue for the
subscription. The protocol manager uses either of the following bits of
information from the frame to determine the routing type:

1. The value of the `subscription-type` header. Valid values are `ANYCAST` and
   `MULTICAST` (case sensitive).

2. The "prefix" on the `destination` header. See [additional
   info](address-model.md#using-prefixes-to-determine-routing-type) on
   prefixes.

If no indication of routing type is supplied then anycast semantics are used.

The `destination` header maps to an address of the same name if multicast is
used or to a queue of the same name if anycast is used. If the `destination`
header used a prefix then the prefix is stripped.

## STOMP heart-beating and connection-ttl

Well behaved STOMP clients will always send a `DISCONNECT` frame before closing
their connections. In this case the server will clear up any server side
resources such as sessions and consumers synchronously. However if STOMP
clients exit without sending a `DISCONNECT` frame or if they crash the server
will have no way of knowing immediately whether the client is still alive or
not. STOMP connections therefore default to a `connection-ttl` value of 1
minute (see chapter on [connection-ttl](connection-ttl.md) for more
information. This value can be overridden using the `connection-ttl-override`
property or if you need a specific connectionTtl for your stomp connections
without affecting the broker-wide `connection-ttl-override` setting, you can
configure your stomp acceptor with the `connectionTtl` property, which is used
to set the ttl for connections that are created from that acceptor. For
example:

```xml
<acceptor name="stomp-acceptor">tcp://localhost:61613?protocols=STOMP;connectionTtl=20000</acceptor>
```

The above configuration will make sure that any STOMP connection that is
created from that acceptor and does not include a `heart-beat` header or
disables client-to-server heart-beats by specifying a `0` value will have its
`connection-ttl` set to 20 seconds. The `connectionTtl` set on an acceptor will
take precedence over `connection-ttl-override`. The default `connectionTtl` is
60,000 milliseconds.

Since STOMP 1.0 does not support heart-beating then all connections from STOMP
1.0 clients will have a connection TTL imposed upon them by the broker based on
the aforementioned configuration options. Likewise, any STOMP 1.1 or 1.2
clients that don't specify a `heart-beat` header or disable client-to-server
heart-beating (e.g. by sending `0,X` in the `heart-beat` header) will have a
connection TTL imposed upon them by the broker.

For STOMP 1.1 and 1.2 clients which send a non-zero client-to-server
`heart-beat` header value then their connection TTL will be set accordingly.
However, the broker will not strictly set the connection TTL to the same value
as the specified in the `heart-beat` since even small network delays could then
cause spurious disconnects. Instead, the client-to-server value in the
`heart-beat` will be multiplied by the `heartBeatConnectionTtlModifer`
specified on the acceptor. The `heartBeatConnectionTtlModifer` is a decimal
value that defaults to `2.0` so for example, if a client sends a `heart-beat`
header of `1000,0` the the connection TTL will be set to `2000` so that the
data or ping frames sent every 1000 milliseconds will have a sufficient cushion
so as not to be considered late and trigger a disconnect. This is also in
accordance with the STOMP 1.1 and 1.2 specifications which both state, "because
of timing inaccuracies, the receiver SHOULD be tolerant and take into account
an error margin."

The minimum and maximum connection TTL allowed can also be specified on the
acceptor via the `connectionTtlMin` and `connectionTtlMax` properties
respectively. The default `connectionTtlMin` is 1000 and the default
`connectionTtlMax` is Java's `Long.MAX_VALUE` meaning there essentially is no
max connection TTL by default. Keep in mind that the
`heartBeatConnectionTtlModifer` is relevant here. For example, if a client
sends a `heart-beat` header of `20000,0` and the acceptor is using a
`connectionTtlMax` of `30000` and a default `heartBeatConnectionTtlModifer` of
`2.0` then the connection TTL would be `40000` (i.e. `20000` * `2.0`) which
would exceed the `connectionTtlMax`. In this case the server would respond to
the client with a `heart-beat` header of `0,15000` (i.e. `30000` / `2.0`). As
described previously, this is to make sure there is a sufficient cushion for
the client heart-beats in accordance with the STOMP 1.1 and 1.2 specifications.
The same kind of calculation is done for `connectionTtlMin`.

The minimum server-to-client heart-beat value is 500ms.

> **Note:**
>
> Please note that the STOMP protocol version 1.0 does not contain any
> heart-beat frame. It is therefore the user's responsibility to make sure data
> is sent within connection-ttl or the server will assume the client is dead
> and clean up server side resources. With STOMP 1.1 users can use heart-beats
> to maintain the life cycle of stomp connections.

## Selector/Filter expressions

STOMP subscribers can specify an expression used to select or filter what the
subscriber receives using the `selector` header. The filter expression syntax
follows the *core filter syntax* described in the [Filter
Expressions](filter-expressions.md) documentation.

## STOMP and JMS interoperability

### Sending and consuming STOMP message from JMS or Core API

STOMP is mainly a text-orientated protocol. To make it simpler to interoperate
with JMS and Core API, our STOMP implementation checks for presence of the
`content-length` header to decide how to map a STOMP 1.0 message to a JMS
Message or a Core message.

If the STOMP 1.0 message does *not* have a `content-length` header, it will be
mapped to a JMS *TextMessage* or a Core message with a *single nullable
SimpleString in the body buffer*.

Alternatively, if the STOMP 1.0 message *has* a `content-length` header, it
will be mapped to a JMS *BytesMessage* or a Core message with a *byte[] in the
body buffer*.

The same logic applies when mapping a JMS message or a Core message to STOMP. A
STOMP 1.0 client can check the presence of the `content-length` header to
determine the type of the message body (String or bytes).

### Message IDs for STOMP messages

When receiving STOMP messages via a JMS consumer or a QueueBrowser, the
messages have no properties like JMSMessageID by default. However this may
bring some inconvenience to clients who wants an ID for their purpose. The
broker STOMP provides a parameter to enable message ID on each incoming STOMP
message. If you want each STOMP message to have a unique ID, just set the
`stompEnableMessageId` to true. For example:

```xml
<acceptor name="stomp-acceptor">tcp://localhost:61613?protocols=STOMP;stompEnableMessageId=true</acceptor>
```

When the server starts with the above setting, each stomp message sent through
this acceptor will have an extra property added. The property key is
`amq-message-id` and the value is a String representation of a long type
internal message id prefixed with `STOMP`, like:

```
amq-message-id : STOMP12345
```

The default `stomp-enable-message-id` value is `false`.

## Durable Subscriptions

The `SUBSCRIBE` and `UNSUBSCRIBE` frames can be augmented with special headers
to create and destroy durable subscriptions respectively.

To create a durable subscription the `client-id` header must be set on the
`CONNECT` frame and the `durable-subscription-name` must be set on the
`SUBSCRIBE` frame. The combination of these two headers will form the identity
of the durable subscription.

To delete a durable subscription the `client-id` header must be set on the
`CONNECT` frame and the `durable-subscription-name` must be set on the
`UNSUBSCRIBE` frame. The values for these headers should match what was set on
the `SUBSCRIBE` frame to delete the corresponding durable subscription.

It is possible to pre-configure durable subscriptions since the STOMP
implementation creates the queue used for the durable subscription in a
deterministic way (i.e. using the format of `client-id`.`subscription-name`).
For example, if you wanted to configure a durable subscription on the address
`myAddress` with a client-id of `myclientid` and a subscription name of
`mysubscription` then configure the durable subscription:

```xml
<addresses>
   <address name="myAddress">
      <multicast>
         <queue name="myclientid.mysubscription"/>
      </multicast>
   </address>
</addresses>
```

## Handling of Large Messages with STOMP

STOMP clients may send very large frame bodies which can exceed the size of the
broker's internal buffer, causing unexpected errors. To prevent this situation
from happening, the broker provides a STOMP configuration attribute
`stompMinLargeMessageSize`. This attribute can be configured inside a stomp
acceptor, as a parameter. For example:

```xml
<acceptor name="stomp-acceptor">tcp://localhost:61613?protocols=STOMP;stompMinLargeMessageSize=10240</acceptor>
```

The type of this attribute is integer. When this attributed is configured, the
broker will check the size of the body of each STOMP frame arrived from
connections established with this acceptor. If the size of the body is equal or
greater than the value of `stompMinLargeMessageSize`, the message will be
persisted as a large message. When a large message is delievered to a STOMP
consumer, the broker will automatically handle the conversion from a large
message to a normal message, before sending it to the client.

If a large message is compressed, the server will uncompressed it before
sending it to stomp clients. The default value of `stompMinLargeMessageSize` is
the same as the default value of
[min-large-message-size](large-messages.md#configuring-parameters).

## Web Sockets

Apache ActiveMQ Artemis also support STOMP over [Web
Sockets](https://html.spec.whatwg.org/multipage/web-sockets.html).  Modern web
browsers which support Web Sockets can send and receive STOMP messages.

STOMP over Web Sockets is supported via the normal STOMP acceptor:

```xml
<acceptor name="stomp-ws-acceptor">tcp://localhost:61614?protocols=STOMP</acceptor>
```

With this configuration, Apache ActiveMQ Artemis will accept STOMP connections
over Web Sockets on the port `61614`. Web browsers can then connect to
`ws://<server>:61614` using a Web Socket to send and receive STOMP messages.

A companion JavaScript library to ease client-side development is available
from [GitHub](https://github.com/jmesnil/stomp-websocket) (please see its
[documentation](http://jmesnil.net/stomp-websocket/doc/) for a complete
description).

The payload length of Web Socket frames can vary between client
implementations. By default the broker will accept frames with a payload length
of 65,536. If the client needs to send payloads longer than this in a single
frame this length can be adjusted by using the `stompMaxFramePayloadLength` URL
parameter on the acceptor.

The `stomp-websockets` example shows how to configure an Apache ActiveMQ
Artemis broker to have web browsers and Java applications exchanges messages.
