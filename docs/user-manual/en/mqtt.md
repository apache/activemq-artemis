# MQTT

MQTT is a light weight, client to server, publish / subscribe messaging
protocol.  MQTT has been specifically designed to reduce transport overhead
(and thus network traffic) and code footprint on client devices.  For this
reason MQTT is ideally suited to constrained devices such as sensors and
actuators and is quickly becoming the defacto standard communication protocol
for IoT.

Apache ActiveMQ Artemis supports the following MQTT versions (with links to
their respective specifications):

 - [3.1](https://public.dhe.ibm.com/software/dw/webservices/ws-mqtt/mqtt-v3r1.html)
 - [3.1.1](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)
 - [5.0](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html)

By default there are `acceptor` elements configured to accept MQTT connections 
on ports `61616` and `1883`.

See the general [Protocols and Interoperability](protocols-interoperability.md)
chapter for details on configuring an `acceptor` for MQTT.

Refer to the MQTT examples for a look at some of this functionality in action.

## MQTT Quality of Service

MQTT offers 3 quality of service levels.

Each message (or topic subscription) can define a quality of service that is
associated with it.  The quality of service level defined on a topic is the
maximum level a client is willing to accept.  The quality of service level on a
message is the desired quality of service level for this message.  The broker
will attempt to deliver messages to subscribers at the highest quality of
service level based on what is defined on the message and topic subscription.

Each quality of service level offers a level of guarantee by which a message is
sent or received:

- QoS 0: `AT MOST ONCE`

  Guarantees that a particular message is only ever received by the subscriber
  a maximum of one time. This does mean that the message may never arrive.  The
  sender and the receiver will attempt to deliver the message, but if something
  fails and the message does not reach its destination (say due to a network
  connection) the message may be lost. This QoS has the least network traffic
  overhead and the least burden on the client and the broker and is often useful
  for telemetry data where it doesn't matter if some of the data is lost.

- QoS 1: `AT LEAST ONCE`

  Guarantees that a message will reach its intended recipient one or more
  times.  The sender will continue to send the message until it receives an
  acknowledgment from the recipient, confirming it has received the message. The
  result of this QoS is that the recipient may receive the message multiple
  times, and also increases the network overhead than QoS 0, (due to acks).  In
  addition more burden is placed on the sender as it needs to store the message
  and retry should it fail to receive an ack in a reasonable time.

- QoS 2: `EXACTLY ONCE`

  The most costly of the QoS (in terms of network traffic and burden on sender
  and receiver) this QoS will ensure that the message is received by a recipient
  exactly one time.  This ensures that the receiver never gets any duplicate
  copies of the message and will eventually get it, but at the extra cost of
  network overhead and complexity required on the sender and receiver.

## MQTT Retain Messages

MQTT has an interesting feature in which messages can be "retained" for a
particular address.  This means that once a retain message has been sent to an
address, any new subscribers to that address will receive the last sent retain
message before any others messages, this happens even if the retained message
was sent before a client has connected or subscribed.  An example of where this
feature might be useful is in environments such as IoT where devices need to
quickly get the current state of a system when they are on boarded into a
system.

## Will Messages

A will message can be sent when a client initially connects to a broker.
Clients are able to set a "will message" as part of the connect packet.  If the
client abnormally disconnects, say due to a device or network failure the
broker will proceed to publish the will message to the specified address (as
defined also in the connect packet). Other subscribers to the will topic will
receive the will message and can react accordingly. This feature can be useful
in an IoT style scenario to detect errors across a potentially large scale
deployment of devices.

## Debug Logging

Detailed protocol logging (e.g. packets in/out) can be activated by turning
on `TRACE` logging for `org.apache.activemq.artemis.core.protocol.mqtt`. Follow
[these steps](logging.md#activating-trace-for-a-specific-logger) to configure
logging appropriately.

The MQTT specification doesn't dictate the format of the payloads which clients
publish. As far as the broker is concerned a payload is just an array of
bytes. However, to facilitate logging the broker will encode the payloads as
UTF-8 strings and print them up to 256 characters. Payload logging is limited
to avoid filling the logs with potentially hundreds of megabytes of unhelpful
information.

## Custom Client ID Handling

The client ID used by an MQTT application is very important as it uniquely
identifies the application. In some situations broker administrators may want
to perform extra validation or even modify incoming client IDs to support
specific use-cases. This is possible by implementing a custom security manager
as demonstrated in the `security-manager` example shipped with the broker.

The simplest implementation is a "wrapper" just like the `security-manager`
example uses. In the `authenticate` method you can modify the client ID using
`setClientId()` on the `org.apache.activemq.artemis.spi.core.protocol.RemotingConnection`
that is passed in. If you perform some custom validation of the client ID you
can reject the client ID by throwing a `org.apache.activemq.artemis.core.protocol.mqtt.exceptions.InvalidClientIdException`.

## Wildcard subscriptions

MQTT addresses are hierarchical much like a file system, and they use a special
character (i.e. `/` by default) to separate hierarchical levels. Subscribers
are able to subscribe to specific topics or to whole branches of a hierarchy.

To subscribe to branches of an address hierarchy a subscriber can use wild
cards. There are 2 types of wildcards in MQTT:

- **Multi level** (`#`)

  Adding this wildcard to an address would match all branches of the address
  hierarchy under a specified node.  For example: `/uk/#`  Would match
  `/uk/cities`, `/uk/cities/newcastle` and also `/uk/rivers/tyne`. Subscribing to
  an address `#` would result in subscribing to all topics in the broker.  This
  can be useful, but should be done so with care since it has significant
  performance implications.

- **Single level** (`+`)

  Matches a single level in the address hierarchy. For example `/uk/+/stores`
  would match `/uk/newcastle/stores` but not `/uk/cities/newcastle/stores`.

These MQTT-specific wildcards are automatically *translated* into the wildcard
syntax used by ActiveMQ Artemis. These wildcards are configurable. See the
[Wildcard Syntax](wildcard-syntax.md#customizing-the-syntax) chapter for details about
how to configure custom wildcards. 

## Web Sockets

Apache ActiveMQ Artemis also supports MQTT over [Web
Sockets](https://html.spec.whatwg.org/multipage/web-sockets.html).  Modern web
browsers which support Web Sockets can send and receive MQTT messages.

MQTT over Web Sockets is supported via a normal MQTT acceptor:

```xml
<acceptor name="mqtt-ws-acceptor">tcp://localhost:1883?protocols=MQTT</acceptor>
```

With this configuration, Apache ActiveMQ Artemis will accept MQTT connections
over Web Sockets on the port `1883`. Web browsers can then connect to
`ws://<server>:1883` using a Web Socket to send and receive MQTT messages.

## Automatic Subscription Clean-up

Sometimes MQTT clients don't clean up their subscriptions. In such situations
the `auto-delete-queues-delay` and `auto-delete-queues-message-count`
address-settings can be used to clean up the abandoned subscription queues.
However, the MQTT session meta-data is still present in memory and needs to be
cleaned up as well. The URL parameter `defaultMqttSessionExpiryInterval` can be
configured on the MQTT `acceptor` to deal with this situation.

MQTT 5 added a new [session expiry interval](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901048)
property with the same basic semantics. The broker will use the client's value
for this property if it is set. If it is not set then it will apply the
`defaultMqttSessionExpiryInterval`.

The default `defaultMqttSessionExpiryInterval` is `-1` which means no MQTT 3.x
session states will be expired and no MQTT 5 session states which do not pass
their own session expiry interval will be expired. Otherwise it represents the
number of **seconds** which must elapse after the client has disconnected
before the broker will remove the session state.

MQTT session state is scanned every 5,000 milliseconds by default. This can be
changed using the `mqtt-session-scan-interval` element set in the `core` section
of `broker.xml`.

## Flow Control

MQTT 5 introduced a simple form of [flow control](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Flow_Control).
In short, a broker can tell a client how many QoS 1 & 2 messages it can receive
before being acknowledged and vice versa.

This is controlled on the broker by setting the `receiveMaximum` URL parameter on
the MQTT `acceptor` in `broker.xml`.

The default value is `65535` (the maximum value of the 2-byte integer used by 
MQTT). 

A value of `0` is prohibited by the MQTT 5 specification.

A value of `-1` will prevent the broker from informing the client of any receive
maximum which means flow-control will be disabled from clients to the broker.
This is effectively the same as setting the value to `65535`, but reduces the size
of the `CONNACK` packet by a few bytes.

## Topic Alias Maximum

MQTT 5 introduced [topic aliasing](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Topic_Alias).
This is an optimization for the size of `PUBLISH` control packets as a 2-byte
integer value can now be substituted for the _name_ of the topic which can
potentially be quite long.

Both the client and the broker can inform each other about the _maximum_ alias
value they support (i.e. how many different aliases they support). This is
controlled on the broker using the `topicAliasMaximum` URL parameter on the
`acceptor` used by the MQTT client.

The default value is `65535` (the maximum value of the 2-byte integer used by
MQTT). 

A value of `0` will disable topic aliasing from clients to the broker.

A value of `-1` will prevent the broker from informing the client of any topic
alias maximum which means aliasing will be disabled from clients to the broker.
This is effectively the same as setting the value to `0`, but reduces the size
of the `CONNACK` packet by a few bytes.

## Maximum Packet Size

MQTT 5 introduced the [maximum packet size](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901086).
This is the maximum packet size the server or client is willing to accept.

This is controlled on the broker by setting the `maximumPacketSize` URL parameter
on the MQTT `acceptor` in `broker.xml`.

The default value is `268435455` (i.e. 256MB - the maximum value of the variable
byte integer used by MQTT). 

A value of `0` is prohibited by the MQTT 5 specification.

A value of `-1` will prevent the broker from informing the client of any maximum
packet size which means no limit will be enforced on the size of incoming packets.
This also reduces the size of the `CONNACK` packet by a few bytes.

## Server Keep Alive

All MQTT versions support a connection keep alive value defined by the *client*.
MQTT 5 introduced a [server keep alive](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901094)
value so that a broker can define the value that the client should use. The 
primary use of the server keep alive is for the server to inform the client that
it will disconnect the client for inactivity sooner than the keep alive specified
by the client.

This is controlled on the broker by setting the `serverKeepAlive` URL parameter
on the MQTT `acceptor` in `broker.xml`.

The default value is `60` and is measured in **seconds**.

A value of `0` completely disables keep alives no matter the client's keep alive
value. This is **not recommended** because disabling keep alives is generally
considered dangerous since it could lead to resource exhaustion.

A value of `-1` means the broker will *always* accept the client's keep alive
value (even if that value is `0`).

Any other value means the `serverKeepAlive` will be applied if it is *less than*
the client's keep alive value **unless** the client's keep alive value is `0` in
which case the `serverKeepAlive` is applied. This is because a value of `0` would
disable keep alives and disabling keep alives is generally considered dangerous
since it could lead to resource exhaustion.

## Enhanced Authentication

MQTT 5 introduced [enhanced authentication](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901256)
which extends the existing name & password authentication to include challenge /
response style authentication.

However, there are currently no challenge / response mechanisms implemented so if
a client passes the "Authentication Method" property in its `CONNECT` packet it will
receive a `CONNACK` with a reason code of `0x8C` (i.e. bad authentication method)
and the network connection will be closed.

## Publish Authorization Failures

The MQTT 3.1.1 specification is ambiguous regarding the broker's behavior when
a `PUBLISH` packet fails due to a lack of authorization. In [section 3.3.5](http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html#_Toc398718042)
it says:

> If a Server implementation does not authorize a PUBLISH to be performed by a
> Client; it has no way of informing that Client. It MUST either make a positive
> acknowledgement, according to the normal QoS rules, or close the Network
> Connection

By default the broker will close the network connection. However if you'd rather
have the broker make a positive acknowledgement then set the URL parameter
`closeMqttConnectionOnPublishAuthorizationFailure` to `false` on the relevant
MQTT `acceptor` in `broker.xml`, e.g.:

```xml
<acceptor name="mqtt">tcp://0.0.0:1883?protocols=MQTT;closeMqttConnectionOnPublishAuthorizationFailure=false</acceptor>
```