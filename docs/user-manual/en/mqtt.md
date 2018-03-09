# MQTT

MQTT is a light weight, client to server, publish / subscribe messaging
protocol.  MQTT has been specifically designed to reduce transport overhead
(and thus network traffic) and code footprint on client devices.  For this
reason MQTT is ideally suited to constrained devices such as sensors and
actuators and is quickly becoming the defacto standard communication protocol
for IoT.

Apache ActiveMQ Artemis supports MQTT v3.1.1 (and also the older v3.1 code
message format). By default there are `acceptor` elements configured to accept
MQTT connections on ports `61616` and `1883`.

See the general [Protocols and Interoperability](protocols-interoperability.md)
chapter for details on configuring an `acceptor` for MQTT.

The best source of information on the MQTT protocol is in the [3.1.1
specification](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html).

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
  fails and the message does not reach it's destination (say due to a network
  connection) the message may be lost. This QoS has the least network traffic
  overhead and the least burden on the client and the broker and is often useful
  for telemetry data where it doesn't matter if some of the data is lost.

- QoS 1: `AT LEAST ONCE`

  Guarantees that a message will reach it's intended recipient one or more
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

Detailed protocol logging (e.g. packets in/out) can be activated via the
following steps:

1. Open `<ARTEMIS_INSTANCE>/etc/logging.properties`

2. Add `org.apache.activemq.artemis.core.protocol.mqtt` to the `loggers` list.

3. Add this line to enable `TRACE` logging for this new logger: 
   `logger.org.apache.activemq.artemis.core.protocol.mqtt.level=TRACE`

4. Ensure the `level` for the `handler` you want to log the message doesn't 
   block the `TRACE` logging. For example, modify the `level` of the `CONSOLE` 
   `handler` like so: `handler.CONSOLE.level=TRACE`.

The MQTT specification doesn't dictate the format of the payloads which clients
publish. As far as the broker is concerned a payload is just just an array of
bytes. However, to facilitate logging the broker will encode the payloads as
UTF-8 strings and print them up to 256 characters. Payload logging is limited
to avoid filling the logs with potentially hundreds of megabytes of unhelpful
information.


## Wild card subscriptions

MQTT addresses are hierarchical much like a file system, and they use a special
character (i.e. `/` by default) to separate hierarchical levels. Subscribers
are able to subscribe to specific topics or to whole branches of a hierarchy.

To subscribe to branches of an address hierarchy a subscriber can use wild
cards. These wild cards (including the aforementioned separator) are
configurable. See the [Wildcard
Syntax](wildcard-syntax.md#customizing-the-syntax) chapter for details about
how to configure custom wild cards.

There are 2 types of wild cards in MQTT:

- **Multi level** (`#` by default)

  Adding this wild card to an address would match all branches of the address
  hierarchy under a specified node.  For example: `/uk/#`  Would match
  `/uk/cities`, `/uk/cities/newcastle` and also `/uk/rivers/tyne`. Subscribing to
  an address `#` would result in subscribing to all topics in the broker.  This
  can be useful, but should be done so with care since it has significant
  performance implications.

- **Single level** (`+` by default)

  Matches a single level in the address hierarchy. For example `/uk/+/stores`
  would match `/uk/newcastle/stores` but not `/uk/cities/newcastle/stores`.

