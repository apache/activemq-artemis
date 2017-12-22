# Messaging Concepts

Apache ActiveMQ Artemis is an asynchronous messaging system, an example of [Message
Oriented
Middleware](https://en.wikipedia.org/wiki/Message-oriented_middleware) ,
we'll just call them messaging systems in the remainder of this book.

We'll first present a brief overview of what kind of things messaging
systems do, where they're useful and the kind of concepts you'll hear
about in the messaging world.

If you're already familiar with what a messaging system is and what it's
capable of, then you can skip this chapter.

## Messaging Concepts

Messaging systems allow you to loosely couple heterogeneous systems
together, whilst typically providing reliability, transactions and many
other features.

Unlike systems based on a [Remote Procedure
Call](https://en.wikipedia.org/wiki/Remote_procedure_call) (RPC) pattern,
messaging systems primarily use an asynchronous message passing pattern
with no tight relationship between requests and responses. Most
messaging systems also support a request-response mode but this is not a
primary feature of messaging systems.

Designing systems to be asynchronous from end-to-end allows you to
really take advantage of your hardware resources, minimizing the amount
of threads blocking on IO operations, and to use your network bandwidth
to its full capacity. With an RPC approach you have to wait for a
response for each request you make so are limited by the network round
trip time, or *latency* of your network. With an asynchronous system you
can pipeline flows of messages in different directions, so are limited
by the network *bandwidth* not the latency. This typically allows you to
create much higher performance applications.

Messaging systems decouple the senders of messages from the consumers of
messages. The senders and consumers of messages are completely
independent and know nothing of each other. This allows you to create
flexible, loosely coupled systems.

Often, large enterprises use a messaging system to implement a message
bus which loosely couples heterogeneous systems together. Message buses
often form the core of an [Enterprise Service
Bus](https://en.wikipedia.org/wiki/Enterprise_service_bus). (ESB). Using
a message bus to de-couple disparate systems can allow the system to
grow and adapt more easily. It also allows more flexibility to add new
systems or retire old ones since they don't have brittle dependencies on
each other.

## Messaging styles

Messaging systems normally support two main styles of asynchronous
messaging: [message queue](https://en.wikipedia.org/wiki/Message_queue)
messaging (also known as *point-to-point messaging*) and [publish
subscribe](https://en.wikipedia.org/wiki/Publish_subscribe) messaging.
We'll summarise them briefly here:

### The Message Queue Pattern

With this type of messaging you send a message to a queue. The message
is then typically persisted to provide a guarantee of delivery, then
some time later the messaging system delivers the message to a consumer.
The consumer then processes the message and when it is done, it
acknowledges the message. Once the message is acknowledged it disappears
from the queue and is not available to be delivered again. If the system
crashes before the messaging server receives an acknowledgement from the
consumer, then on recovery, the message will be available to be
delivered to a consumer again.

With point-to-point messaging, there can be many consumers on the queue
but a particular message will only ever be consumed by a maximum of one
of them. Senders (also known as *producers*) to the queue are completely
decoupled from receivers (also known as *consumers*) of the queue - they
do not know of each other's existence.

A classic example of point to point messaging would be an order queue in
a company's book ordering system. Each order is represented as a message
which is sent to the order queue. Let's imagine there are many front end
ordering systems which send orders to the order queue. When a message
arrives on the queue it is persisted - this ensures that if the server
crashes the order is not lost. Let's also imagine there are many
consumers on the order queue - each representing an instance of an order
processing component - these can be on different physical machines but
consuming from the same queue. The messaging system delivers each
message to one and only one of the ordering processing components.
Different messages can be processed by different order processors, but a
single order is only processed by one order processor - this ensures
orders aren't processed twice.

As an order processor receives a message, it fulfills the order, sends
order information to the warehouse system and then updates the order
database with the order details. Once it's done that it acknowledges the
message to tell the server that the order has been processed and can be
forgotten about. Often the send to the warehouse system, update in
database and acknowledgement will be completed in a single transaction
to ensure [ACID](https://en.wikipedia.org/wiki/ACID) properties.

### The Publish-Subscribe Pattern

With publish-subscribe messaging many senders can send messages to an
entity on the server, often called a *topic* (e.g. in the JMS world).

There can be many *subscriptions* on a topic, a subscription is just
another word for a consumer of a topic. Each subscription receives a
*copy* of *each* message sent to the topic. This differs from the
message queue pattern where each message is only consumed by a single
consumer.

Subscriptions can optionally be *durable* which means they retain a copy
of each message sent to the topic until the subscriber consumes them -
even if the server crashes or is restarted in between. Non-durable
subscriptions only last a maximum of the lifetime of the connection that
created them.

An example of publish-subscribe messaging would be a news feed. As news
articles are created by different editors around the world they are sent
to a news feed topic. There are many subscribers around the world who
are interested in receiving news items - each one creates a subscription
and the messaging system ensures that a copy of each news message is
delivered to each subscription.

## Delivery guarantees

A key feature of most messaging systems is *reliable messaging*. With
reliable messaging the server gives a guarantee that the message will be
delivered once and only once to each consumer of a queue or each durable
subscription of a topic, even in the event of system failure. This is
crucial for many businesses; e.g. you don't want your orders fulfilled
more than once or any of your orders to be lost.

In other cases you may not care about a once and only once delivery
guarantee and are happy to cope with duplicate deliveries or lost
messages - an example of this might be transient stock price updates -
which are quickly superseded by the next update on the same stock. The
messaging system allows you to configure which delivery guarantees you
require.

## Transactions

Messaging systems typically support the sending and acknowledgement of
multiple messages in a single local transaction. Apache ActiveMQ Artemis also supports
the sending and acknowledgement of message as part of a large global
transaction - using the Java mapping of XA: JTA.

## Durability

Messages are either durable or non durable. Durable messages will be
persisted in permanent storage and will survive server failure or
restart. Non durable messages will not survive server failure or
restart. Examples of durable messages might be orders or trades, where
they cannot be lost. An example of a non durable message might be a
stock price update which is transitory and doesn't need to survive a
restart.

## Messaging APIs and protocols

How do client applications interact with messaging systems in order to
send and consume messages?

Several messaging systems provide their own proprietary APIs with which
the client communicates with the messaging system.

There are also some standard ways of operating with messaging systems
and some emerging standards in this space.

Let's take a brief look at these:

### Java Message Service (JMS)

[JMS](https://en.wikipedia.org/wiki/Java_Message_Service) is part of
Oracle's Java EE specification. It's a Java API that encapsulates both message
queue and publish-subscribe messaging patterns. JMS is a lowest common
denominator specification - i.e. it was created to encapsulate common
functionality of the already existing messaging systems that were
available at the time of its creation.

JMS is a very popular API and is implemented by most messaging systems.
JMS is only available to clients running Java.

JMS does not define a standard wire format - it only defines a
programmatic API so JMS clients and servers from different vendors
cannot directly interoperate since each will use the vendor's own
internal wire protocol.

Apache ActiveMQ Artemis provides a fully compliant JMS 1.1 and JMS 2.0 API.

### System specific APIs

Many systems provide their own programmatic API for which to interact
with the messaging system. The advantage of this it allows the full set
of system functionality to be exposed to the client application. API's
like JMS are not normally rich enough to expose all the extra features
that most messaging systems provide.

Apache ActiveMQ Artemis provides its own core client API for clients to use if they
wish to have access to functionality over and above that accessible via
the JMS API.

### RESTful API

[REST](https://en.wikipedia.org/wiki/Representational_State_Transfer)
approaches to messaging are showing a lot interest recently.

It seems plausible that API standards for cloud computing may converge
on a REST style set of interfaces and consequently a REST messaging
approach is a very strong contender for becoming the de-facto method for
messaging interoperability.

With a REST approach messaging resources are manipulated as resources
defined by a URI and typically using a simple set of operations on those
resources, e.g. PUT, POST, GET etc. REST approaches to messaging often
use HTTP as their underlying protocol.

The advantage of a REST approach with HTTP is in its simplicity and the
fact the internet is already tuned to deal with HTTP optimally.

Please see [Rest Interface](rest.md) for using Apache ActiveMQ Artemis's RESTful interface.

### AMQP

[AMQP](https://en.wikipedia.org/wiki/AMQP) is a specification for
interoperable messaging. It also defines a wire format, so any AMQP
client can work with any messaging system that supports AMQP. AMQP
clients are available in many different programming languages.

Apache ActiveMQ Artemis implements the [AMQP
1.0](https://www.oasis-open.org/committees/tc_home.php?wg_abbrev=amqp)
specification. Any client that supports the 1.0 specification will be
able to interact with Apache ActiveMQ Artemis.

### MQTT
[MQTT](https://mqtt.org/) is a lightweight connectivity protocol.  It is designed
to run in environments where device and networks are constrained.  Out of the box
Apache ActiveMQ Artemis supports version MQTT 3.1.1.  Any client supporting this
version of the protocol will work against Apache ActiveMQ Artemis.

### STOMP

[Stomp](https://stomp.github.io/) is a very simple text protocol for
interoperating with messaging systems. It defines a wire format, so
theoretically any Stomp client can work with any messaging system that
supports Stomp. Stomp clients are available in many different
programming languages.

Please see [Stomp](protocols-interoperability.md) for using STOMP with Apache ActiveMQ Artemis.

### OPENWIRE

ActiveMQ 5.x defines it's own wire Protocol "OPENWIRE".  In order to support 
ActiveMQ 5.x clients, Apache ActiveMQ Artemis supports OPENWIRE.  Any ActiveMQ 5.12.x
or higher can be used with Apache ActiveMQ Artemis.

## High Availability

High Availability (HA) means that the system should remain operational
after failure of one or more of the servers. The degree of support for
HA varies between various messaging systems.

Apache ActiveMQ Artemis provides automatic failover where your sessions are
automatically reconnected to the backup server on event of live server
failure.

For more information on HA, please see [High Availability and Failover](ha.md).

## Clusters

Many messaging systems allow you to create groups of messaging servers
called *clusters*. Clusters allow the load of sending and consuming
messages to be spread over many servers. This allows your system to
scale horizontally by adding new servers to the cluster.

Degrees of support for clusters varies between messaging systems, with
some systems having fairly basic clusters with the cluster members being
hardly aware of each other.

Apache ActiveMQ Artemis provides very configurable state-of-the-art clustering model
where messages can be intelligently load balanced between the servers in
the cluster, according to the number of consumers on each node, and
whether they are ready for messages.

Apache ActiveMQ Artemis also has the ability to automatically redistribute messages
between nodes of a cluster to prevent starvation on any particular node.

For full details on clustering, please see [Clusters](clusters.md).

## Bridges and routing

Some messaging systems allow isolated clusters or single nodes to be
bridged together, typically over unreliable connections like a wide area
network (WAN), or the internet.

A bridge normally consumes from a queue on one server and forwards
messages to another queue on a different server. Bridges cope with
unreliable connections, automatically reconnecting when the connections
becomes available again.

Apache ActiveMQ Artemis bridges can be configured with filter expressions to only
forward certain messages, and transformation can also be hooked in.

Apache ActiveMQ Artemis also allows routing between queues to be configured in server
side configuration. This allows complex routing networks to be set up
forwarding or copying messages from one destination to another, forming
a global network of interconnected brokers.

For more information please see [Core Bridges](core-bridges.md) and [Diverting and Splitting Message Flows](diverts.md).
