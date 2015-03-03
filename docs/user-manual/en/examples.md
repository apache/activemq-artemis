Examples
========

The Apache ActiveMQ distribution comes with over 90 run out-of-the-box examples
demonstrating many of the features.

The examples are available in the distribution, in the `examples`
directory. Examples are split into JMS and core examples. JMS examples
show how a particular feature can be used by a normal JMS client. Core
examples show how the equivalent feature can be used by a core messaging
client.

A set of Java EE examples are also provided which need WildFly installed
to be able to run.

JMS Examples
============

To run a JMS example, simply `cd` into the appropriate example directory
and type `mvn verify` (For details please read the readme.html in each
example directory).

Here's a listing of the examples with a brief description.

JMS AeroGear
------------

This example shows how you can send a message to a mobile device by
leveraging AeroGears push technology which provides support for
different push notification technologies like Google Cloud Messaging,
Apple's APNs or Mozilla's SimplePush.

Applet
------

This example shows you how to send and receive JMS messages from an
Applet.

Application-Layer Failover
--------------------------

Apache ActiveMQ also supports Application-Layer failover, useful in the case
that replication is not enabled on the server side.

With Application-Layer failover, it's up to the application to register
a JMS `ExceptionListener` with Apache ActiveMQ which will be called by Apache ActiveMQ
in the event that connection failure is detected.

The code in the `ExceptionListener` then recreates the JMS connection,
session, etc on another node and the application can continue.

Application-layer failover is an alternative approach to High
Availability (HA). Application-layer failover differs from automatic
failover in that some client side coding is required in order to
implement this. Also, with Application-layer failover, since the old
session object dies and a new one is created, any uncommitted work in
the old session will be lost, and any unacknowledged messages might be
redelivered.

Core Bridge Example
-------------------

The `bridge` example demonstrates a core bridge deployed on one server,
which consumes messages from a local queue and forwards them to an
address on a second server.

Core bridges are used to create message flows between any two Apache ActiveMQ
servers which are remotely separated. Core bridges are resilient and
will cope with temporary connection failure allowing them to be an ideal
choice for forwarding over unreliable connections, e.g. a WAN.

Browser
-------

The `browser` example shows you how to use a JMS `QueueBrowser` with
Apache ActiveMQ.

Queues are a standard part of JMS, please consult the JMS 1.1
specification for full details.

A `QueueBrowser` is used to look at messages on the queue without
removing them. It can scan the entire content of a queue or only
messages matching a message selector.

Client Kickoff
--------------

The `client-kickoff` example shows how to terminate client connections
given an IP address using the JMX management API.

Client side failover listener
-----------------------------

The `client-side-failoverlistener` example shows how to register a
listener to monitor failover events

Client-Side Load-Balancing
--------------------------

The `client-side-load-balancing` example demonstrates how sessions
created from a single JMS `Connection` can be created to different nodes
of the cluster. In other words it demonstrates how Apache ActiveMQ does
client-side load-balancing of sessions across the cluster.

Clustered Durable Subscription
------------------------------

This example demonstrates a clustered JMS durable subscription

Clustered Grouping
------------------

This is similar to the message grouping example except that it
demonstrates it working over a cluster. Messages sent to different nodes
with the same group id will be sent to the same node and the same
consumer.

Clustered Queue
---------------

The `clustered-queue` example demonstrates a JMS queue deployed on two
different nodes. The two nodes are configured to form a cluster. We then
create a consumer for the queue on each node, and we create a producer
on only one of the nodes. We then send some messages via the producer,
and we verify that both consumers receive the sent messages in a
round-robin fashion.

Clustering with JGroups
-----------------------

The `clustered-jgroups` example demonstrates how to form a two node
cluster using JGroups as its underlying topology discovery technique,
rather than the default UDP broadcasting. We then create a consumer for
the queue on each node, and we create a producer on only one of the
nodes. We then send some messages via the producer, and we verify that
both consumers receive the sent messages in a round-robin fashion.

Clustered Standalone
--------------------

The `clustered-standalone` example demonstrates how to configure and
starts 3 cluster nodes on the same machine to form a cluster. A
subscriber for a JMS topic is created on each node, and we create a
producer on only one of the nodes. We then send some messages via the
producer, and we verify that the 3 subscribers receive all the sent
messages.

Clustered Static Discovery
--------------------------

This example demonstrates how to configure a cluster using a list of
connectors rather than UDP for discovery

Clustered Static Cluster One Way
--------------------------------

This example demonstrates how to set up a cluster where cluster
connections are one way, i.e. server A -\> Server B -\> Server C

Clustered Topic
---------------

The `clustered-topic` example demonstrates a JMS topic deployed on two
different nodes. The two nodes are configured to form a cluster. We then
create a subscriber on the topic on each node, and we create a producer
on only one of the nodes. We then send some messages via the producer,
and we verify that both subscribers receive all the sent messages.

Message Consumer Rate Limiting
------------------------------

With Apache ActiveMQ you can specify a maximum consume rate at which a JMS
MessageConsumer will consume messages. This can be specified when
creating or deploying the connection factory.

If this value is specified then Apache ActiveMQ will ensure that messages are
never consumed at a rate higher than the specified rate. This is a form
of consumer throttling.

Dead Letter
-----------

The `dead-letter` example shows you how to define and deal with dead
letter messages. Messages can be delivered unsuccessfully (e.g. if the
transacted session used to consume them is rolled back).

Such a message goes back to the JMS destination ready to be redelivered.
However, this means it is possible for a message to be delivered again
and again without any success and remain in the destination, clogging
the system.

To prevent this, messaging systems define dead letter messages: after a
specified unsuccessful delivery attempts, the message is removed from
the destination and put instead in a dead letter destination where they
can be consumed for further investigation.

Delayed Redelivery
------------------

The `delayed-redelivery` example demonstrates how Apache ActiveMQ can be
configured to provide a delayed redelivery in the case a message needs
to be redelivered.

Delaying redelivery can often be useful in the case that clients
regularly fail or roll-back. Without a delayed redelivery, the system
can get into a "thrashing" state, with delivery being attempted, the
client rolling back, and delivery being re-attempted in quick
succession, using up valuable CPU and network resources.

Divert
------

Apache ActiveMQ diverts allow messages to be transparently "diverted" or copied
from one address to another with just some simple configuration defined
on the server side.

Durable Subscription
--------------------

The `durable-subscription` example shows you how to use a durable
subscription with Apache ActiveMQ. Durable subscriptions are a standard part of
JMS, please consult the JMS 1.1 specification for full details.

Unlike non-durable subscriptions, the key function of durable
subscriptions is that the messages contained in them persist longer than
the lifetime of the subscriber - i.e. they will accumulate messages sent
to the topic even if there is no active subscriber on them. They will
also survive server restarts or crashes. Note that for the messages to
be persisted, the messages sent to them must be marked as durable
messages.

Embedded
--------

The `embedded` example shows how to embed JMS within your own code using
POJO instantiation and no config files.

Embedded Simple
---------------

The `embedded` example shows how to embed JMS within your own code using
regular Apache ActiveMQ XML files.

Message Expiration
------------------

The `expiry` example shows you how to define and deal with message
expiration. Messages can be retained in the messaging system for a
limited period of time before being removed. JMS specification states
that clients should not receive messages that have been expired (but it
does not guarantee this will not happen).

Apache ActiveMQ can assign an expiry address to a given queue so that when
messages are expired, they are removed from the queue and sent to the
expiry address. These "expired" messages can later be consumed from the
expiry address for further inspection.

Apache ActiveMQ Resource Adapter example
---------------------------------

This examples shows how to build the activemq resource adapters a rar
for deployment in other Application Server's

HTTP Transport
--------------

The `http-transport` example shows you how to configure Apache ActiveMQ to use
the HTTP protocol as its transport layer.

Instantiate JMS Objects Directly
--------------------------------

Usually, JMS Objects such as `ConnectionFactory`, `Queue` and `Topic`
instances are looked up from JNDI before being used by the client code.
This objects are called "administered objects" in JMS terminology.

However, in some cases a JNDI server may not be available or desired. To
come to the rescue Apache ActiveMQ also supports the direct instantiation of
these administered objects on the client side so you don't have to use
JNDI for JMS.

Interceptor
-----------

Apache ActiveMQ allows an application to use an interceptor to hook into the
messaging system. Interceptors allow you to handle various message
events in Apache ActiveMQ.

JAAS
----

The `jaas` example shows you how to configure Apache ActiveMQ to use JAAS for
security. Apache ActiveMQ can leverage JAAS to delegate user authentication and
authorization to existing security infrastructure.

JMS Auto Closable
-----------------

The `jms-auto-closeable` example shows how JMS resources, such as
connections, sessions and consumers, in JMS 2 can be automatically
closed on error.

JMS Completion Listener
-----------------------

The `jms-completion-listener` example shows how to send a message
asynchronously to Apache ActiveMQ and use a CompletionListener to be notified
of the Broker receiving it.

JMS Bridge
----------

The `jms-brige` example shows how to setup a bridge between two
standalone Apache ActiveMQ servers.

JMS Context
-----------

The `jms-context` example shows how to send and receive a message to a
JMS Queue using Apache ActiveMQ by using a JMS Context.

A JMSContext is part of JMS 2.0 and combines the JMS Connection and
Session Objects into a simple Interface.

JMS Shared Consumer
-------------------

The `jms-shared-consumer` example shows you how can use shared consumers
to share a subscription on a topic. In JMS 1.1 this was not allowed and
so caused a scalability issue. In JMS 2 this restriction has been lifted
so you can share the load across different threads and connections.

JMX Management
--------------

The `jmx` example shows how to manage Apache ActiveMQ using JMX.

Large Message
-------------

The `large-message` example shows you how to send and receive very large
messages with Apache ActiveMQ. Apache ActiveMQ supports the sending and receiving of
huge messages, much larger than can fit in available RAM on the client
or server. Effectively the only limit to message size is the amount of
disk space you have on the server.

Large messages are persisted on the server so they can survive a server
restart. In other words Apache ActiveMQ doesn't just do a simple socket stream
from the sender to the consumer.

Last-Value Queue
----------------

The `last-value-queue` example shows you how to define and deal with
last-value queues. Last-value queues are special queues which discard
any messages when a newer message with the same value for a well-defined
last-value property is put in the queue. In other words, a last-value
queue only retains the last value.

A typical example for last-value queue is for stock prices, where you
are only interested by the latest price for a particular stock.

Management
----------

The `management` example shows how to manage Apache ActiveMQ using JMS Messages
to invoke management operations on the server.

Management Notification
-----------------------

The `management-notification` example shows how to receive management
notifications from Apache ActiveMQ using JMS messages. Apache ActiveMQ servers emit
management notifications when events of interest occur (consumers are
created or closed, addresses are created or deleted, security
authentication fails, etc.).

Message Counter
---------------

The `message-counters` example shows you how to use message counters to
obtain message information for a JMS queue.

Message Group
-------------

The `message-group` example shows you how to configure and use message
groups with Apache ActiveMQ. Message groups allow you to pin messages so they
are only consumed by a single consumer. Message groups are sets of
messages that has the following characteristics:

-   Messages in a message group share the same group id, i.e. they have
    same JMSXGroupID string property values

-   The consumer that receives the first message of a group will receive
    all the messages that belongs to the group

Message Group
-------------

The `message-group2` example shows you how to configure and use message
groups with Apache ActiveMQ via a connection factory.

Message Priority
----------------

Message Priority can be used to influence the delivery order for
messages.

It can be retrieved by the message's standard header field 'JMSPriority'
as defined in JMS specification version 1.1.

The value is of type integer, ranging from 0 (the lowest) to 9 (the
highest). When messages are being delivered, their priorities will
effect their order of delivery. Messages of higher priorities will
likely be delivered before those of lower priorities.

Messages of equal priorities are delivered in the natural order of their
arrival at their destinations. Please consult the JMS 1.1 specification
for full details.

Multiple Failover
-----------------

This example demonstrates how to set up a live server with multiple
backups

Multiple Failover Failback
--------------------------

This example demonstrates how to set up a live server with multiple
backups but forcing failover back to the original live server

No Consumer Buffering
---------------------

By default, Apache ActiveMQ consumers buffer messages from the server in a
client side buffer before you actually receive them on the client side.
This improves performance since otherwise every time you called
receive() or had processed the last message in a
`MessageListener onMessage()` method, the Apache ActiveMQ client would have to
go the server to request the next message, which would then get sent to
the client side, if one was available.

This would involve a network round trip for every message and reduce
performance. Therefore, by default, Apache ActiveMQ pre-fetches messages into a
buffer on each consumer.

In some case buffering is not desirable, and Apache ActiveMQ allows it to be
switched off. This example demonstrates that.

Non-Transaction Failover With Server Data Replication
-----------------------------------------------------

The `non-transaction-failover` example demonstrates two servers coupled
as a live-backup pair for high availability (HA), and a client using a
*non-transacted* JMS session failing over from live to backup when the
live server is crashed.

Apache ActiveMQ implements failover of client connections between live and
backup servers. This is implemented by the replication of state between
live and backup nodes. When replication is configured and a live node
crashes, the client connections can carry and continue to send and
consume messages. When non-transacted sessions are used, once and only
once message delivery is not guaranteed and it is possible that some
messages will be lost or delivered twice.

OpenWire
--------

The `Openwire` example shows how to configure an Apache ActiveMQ server to
communicate with an Apache ActiveMQ JMS client that uses open-wire protocol.

Paging
------

The `paging` example shows how Apache ActiveMQ can support huge queues even
when the server is running in limited RAM. It does this by transparently
*paging* messages to disk, and *depaging* them when they are required.

Pre-Acknowledge
---------------

Standard JMS supports three acknowledgement modes:`
                    AUTO_ACKNOWLEDGE`, `CLIENT_ACKNOWLEDGE`, and
`DUPS_OK_ACKNOWLEDGE`. For a full description on these modes please
consult the JMS specification, or any JMS tutorial.

All of these standard modes involve sending acknowledgements from the
client to the server. However in some cases, you really don't mind
losing messages in event of failure, so it would make sense to
acknowledge the message on the server before delivering it to the
client. This example demonstrates how Apache ActiveMQ allows this with an extra
acknowledgement mode.

Message Producer Rate Limiting
------------------------------

The `producer-rte-limit` example demonstrates how, with Apache ActiveMQ, you
can specify a maximum send rate at which a JMS message producer will
send messages.

Proton Qpid
-----------

Apache ActiveMQ can be configured to accept requests from any AMQP client that
supports the 1.0 version of the protocol. This `proton-j` example shows
a simply qpid java 1.0 client example.

Proton Ruby
-----------

Apache ActiveMQ can be configured to accept requests from any AMQP client that
supports the 1.0 version of the protocol. This example shows a simply
proton ruby client that sends and receives messages

Queue
-----

A simple example demonstrating a JMS queue.

Message Redistribution
----------------------

The `queue-message-redistribution` example demonstrates message
redistribution between queues with the same name deployed in different
nodes of a cluster.

Queue Requestor
---------------

A simple example demonstrating a JMS queue requestor.

Queue with Message Selector
---------------------------

The `queue-selector` example shows you how to selectively consume
messages using message selectors with queue consumers.

Reattach Node example
---------------------

The `Reattach Node` example shows how a client can try to reconnect to
the same server instead of failing the connection immediately and
notifying any user ExceptionListener objects. Apache ActiveMQ can be configured
to automatically retry the connection, and reattach to the server when
it becomes available again across the network.

Replicated Failback example
---------------------------

An example showing how failback works when using replication, In this
example a live server will replicate all its Journal to a backup server
as it updates it. When the live server crashes the backup takes over
from the live server and the client reconnects and carries on from where
it left off.

Replicated Failback static example
----------------------------------

An example showing how failback works when using replication, but this
time with static connectors

Replicated multiple failover example
------------------------------------

An example showing how to configure multiple backups when using
replication

Replicated Failover transaction example
---------------------------------------

An example showing how failover works with a transaction when using
replication

Request-Reply example
---------------------

A simple example showing the JMS request-response pattern.

Rest example
------------

An example showing how to use the Apache ActiveMQ Rest API

Scheduled Message
-----------------

The `scheduled-message` example shows you how to send a scheduled
message to a JMS Queue with Apache ActiveMQ. Scheduled messages won't get
delivered until a specified time in the future.

Security
--------

The `security` example shows you how configure and use role based queue
security with Apache ActiveMQ.

Send Acknowledgements
---------------------

The `send-acknowledgements` example shows you how to use Apache ActiveMQ's
advanced *asynchronous send acknowledgements* feature to obtain
acknowledgement from the server that sends have been received and
processed in a separate stream to the sent messages.

Spring Integration
------------------

This example shows how to use embedded JMS using Apache ActiveMQ's Spring
integration.

SSL Transport
-------------

The `ssl-enabled` shows you how to configure SSL with Apache ActiveMQ to send
and receive message.

Static Message Selector
-----------------------

The `static-selector` example shows you how to configure an Apache ActiveMQ core
queue with static message selectors (filters).

Static Message Selector Using JMS
---------------------------------

The `static-selector-jms` example shows you how to configure an Apache ActiveMQ
queue with static message selectors (filters) using JMS.

Stomp
-----

The `stomp` example shows you how to configure an Apache ActiveMQ server to send
and receive Stomp messages.

Stomp1.1
--------

The `stomp` example shows you how to configure an Apache ActiveMQ server to send
and receive Stomp messages via a Stomp 1.1 connection.

Stomp1.2
--------

The `stomp` example shows you how to configure an Apache ActiveMQ server to send
and receive Stomp messages via a Stomp 1.2 connection.

Stomp Over Web Sockets
----------------------

The `stomp-websockets` example shows you how to configure an Apache ActiveMQ
server to send and receive Stomp messages directly from Web browsers
(provided they support Web Sockets).

Symmetric Cluster
-----------------

The `symmetric-cluster` example demonstrates a symmetric cluster set-up
with Apache ActiveMQ.

Apache ActiveMQ has extremely flexible clustering which allows you to set-up
servers in many different topologies. The most common topology that
you'll perhaps be familiar with if you are used to application server
clustering is a symmetric cluster.

With a symmetric cluster, the cluster is homogeneous, i.e. each node is
configured the same as every other node, and every node is connected to
every other node in the cluster.

Temporary Queue
---------------

A simple example demonstrating how to use a JMS temporary queue.

Topic
-----

A simple example demonstrating a JMS topic.

Topic Hierarchy
---------------

Apache ActiveMQ supports topic hierarchies. With a topic hierarchy you can
register a subscriber with a wild-card and that subscriber will receive
any messages sent to an address that matches the wild card.

Topic Selector 1
----------------

The `topic-selector-example1` example shows you how to send message to a
JMS Topic, and subscribe them using selectors with Apache ActiveMQ.

Topic Selector 2
----------------

The `topic-selector-example2` example shows you how to selectively
consume messages using message selectors with topic consumers.

Transaction Failover
--------------------

The `transaction-failover` example demonstrates two servers coupled as a
live-backup pair for high availability (HA), and a client using a
transacted JMS session failing over from live to backup when the live
server is crashed.

Apache ActiveMQ implements failover of client connections between live and
backup servers. This is implemented by the sharing of a journal between
the servers. When a live node crashes, the client connections can carry
and continue to send and consume messages. When transacted sessions are
used, once and only once message delivery is guaranteed.

Failover Without Transactions
-----------------------------

The `stop-server-failover` example demonstrates failover of the JMS
connection from one node to another when the live server crashes using a
JMS non-transacted session.

Transactional Session
---------------------

The `transactional` example shows you how to use a transactional Session
with Apache ActiveMQ.

XA Heuristic
------------

The `xa-heuristic` example shows you how to make an XA heuristic
decision through Apache ActiveMQ Management Interface. A heuristic decision is
a unilateral decision to commit or rollback an XA transaction branch
after it has been prepared.

XA Receive
----------

The `xa-receive` example shows you how message receiving behaves in an
XA transaction in Apache ActiveMQ.

XA Send
-------

The `xa-send` example shows you how message sending behaves in an XA
transaction in Apache ActiveMQ.
Core API Examples
=================

To run a core example, simply `cd` into the appropriate example
directory and type `ant`

Embedded
--------

The `embedded` example shows how to embed the Apache ActiveMQ server within
your own code.
