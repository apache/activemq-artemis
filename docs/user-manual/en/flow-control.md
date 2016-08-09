# Flow Control

Flow control is used to limit the flow of data between a client and
server, or a server and another server in order to prevent the client or
server being overwhelmed with data.

## Consumer Flow Control

This controls the flow of data between the server and the client as the
client consumes messages. For performance reasons clients normally
buffer messages before delivering to the consumer via the `receive()`
method or asynchronously via a message listener. If the consumer cannot
process messages as fast as they are being delivered and stored in the
internal buffer, then you could end up with a situation where messages
would keep building up possibly causing out of memory on the client if
they cannot be processed in time.

## Window-Based Flow Control

By default, Apache ActiveMQ Artemis consumers buffer messages from the server in a
client side buffer before the client consumes them. This improves
performance: otherwise every time the client consumes a message,
Apache ActiveMQ Artemis would have to go the server to request the next message. In
turn, this message would then get sent to the client side, if one was
available.

A network round trip would be involved for *every* message and
considerably reduce performance.

To prevent this, Apache ActiveMQ Artemis pre-fetches messages into a buffer on each
consumer. The total maximum size of messages (in bytes) that will be
buffered on each consumer is determined by the `consumerWindowSize`
parameter.

By default, the `consumerWindowSize` is set to 1 MiB (1024 \* 1024
bytes).

The value can be:

-   `-1` for an *unbounded* buffer

-   `0` to not buffer any messages.

-   `>0` for a buffer with the given maximum size in bytes.

Setting the consumer window size can considerably improve performance
depending on the messaging use case. As an example, let's consider the
two extremes:

### Fast consumers
Fast consumers can process messages as fast as they consume them (or
even faster)

To allow fast consumers, set the `consumerWindowSize` to -1. This
will allow *unbounded* message buffering on the client side.

Use this setting with caution: it can overflow the client memory if
the consumer is not able to process messages as fast as it receives
them.

### Slow consumers
Slow consumers takes significant time to process each message and it
is desirable to prevent buffering messages on the client side so
that they can be delivered to another consumer instead.

Consider a situation where a queue has 2 consumers; 1 of which is
very slow. Messages are delivered in a round robin fashion to both
consumers, the fast consumer processes all of its messages very
quickly until its buffer is empty. At this point there are still
messages awaiting to be processed in the buffer of the slow consumer
thus preventing them being processed by the fast consumer. The fast
consumer is therefore sitting idle when it could be processing the
other messages.

To allow slow consumers, set the `consumerWindowSize` to 0 (for no
buffer at all). This will prevent the slow consumer from buffering
any messages on the client side. Messages will remain on the server
side ready to be consumed by other consumers.

Setting this to 0 can give deterministic distribution between
multiple consumers on a queue.

Most of the consumers cannot be clearly identified as fast or slow
consumers but are in-between. In that case, setting the value of
`consumerWindowSize` to optimize performance depends on the messaging
use case and requires benchmarks to find the optimal value, but a value
of 1MiB is fine in most cases.

### Using Core API

If Apache ActiveMQ Artemis Core API is used, the consumer window size is specified by
`ServerLocator.setConsumerWindowSize()` method and some of the
`ClientSession.createConsumer()` methods.

### Using JMS

If JNDI is used on the client to instantiate and look up the connection
factory the consumer window size is configured in the JNDI context
environment, e.g. `jndi.properties`. Here's a simple example using the
"ConnectionFactory" connection factory which is available in the context
by default:

    java.naming.factory.initial=org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory
    connectionFactory.myConnectionFactory=tcp://localhost:61616?consumerWindowSize=0

If the connection factory is directly instantiated, the consumer window
size is specified by `ActiveMQConnectionFactory.setConsumerWindowSize()`
method.

Please see the examples for an example which shows how to configure Apache ActiveMQ Artemis to
prevent consumer buffering when dealing with slow consumers.

## Rate limited flow control

It is also possible to control the *rate* at which a consumer can
consume messages. This is a form of throttling and can be used to make
sure that a consumer never consumes messages at a rate faster than the
rate specified.

The rate must be a positive integer to enable this functionality and is
the maximum desired message consumption rate specified in units of
messages per second. Setting this to `-1` disables rate limited flow
control. The default value is `-1`.

Please see [the examples chapter](examples.md) for a working example of limiting consumer rate.

### Using Core API

If the Apache ActiveMQ Artemis core API is being used the rate can be set via the
`ServerLocator.setConsumerMaxRate(int consumerMaxRate)` method or
alternatively via some of the `ClientSession.createConsumer()` methods.

### Using JMS

If JNDI is used to instantiate and look up the connection factory, the
max rate can be configured in the JNDI context environment, e.g.
`jndi.properties`. Here's a simple example using the "ConnectionFactory"
connection factory which is available in the context by default:

    java.naming.factory.initial=org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory
    java.naming.provider.url=tcp://localhost:61616?consumerMaxRate=10

If the connection factory is directly instantiated, the max rate size
can be set via the `ActiveMQConnectionFactory.setConsumerMaxRate(int
                  consumerMaxRate)` method.

> **Note**
>
> Rate limited flow control can be used in conjunction with window based
> flow control. Rate limited flow control only effects how many messages
> a client can consume in a second and not how many messages are in its
> buffer. So if you had a slow rate limit and a high window based limit
> the clients internal buffer would soon fill up with messages.

Please see [the examples chapter](examples.md) for an example which shows how to configure ActiveMQ Artemis to
prevent consumer buffering when dealing with slow consumers.

## Producer flow control

Apache ActiveMQ Artemis also can limit the amount of data sent from a client to a
server to prevent the server being overwhelmed.

### Window based flow control

In a similar way to consumer window based flow control, Apache ActiveMQ Artemis
producers, by default, can only send messages to an address as long as
they have sufficient credits to do so. The amount of credits required to
send a message is given by the size of the message.

As producers run low on credits they request more from the server, when
the server sends them more credits they can send more messages.

The amount of credits a producer requests in one go is known as the
*window size*.

The window size therefore determines the amount of bytes that can be
in-flight at any one time before more need to be requested - this
prevents the remoting connection from getting overloaded.

#### Using Core API

If the Apache ActiveMQ Artemis core API is being used, window size can be set via the
`ServerLocator.setProducerWindowSize(int producerWindowSize)` method.

#### Using JMS

If JNDI is used to instantiate and look up the connection factory, the
producer window size can be configured in the JNDI context environment,
e.g. `jndi.properties`. Here's a simple example using the
"ConnectionFactory" connection factory which is available in the context
by default:

    java.naming.factory.initial=org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory
    connectionFactory.myConnectionFactory=tcp://localhost:61616?producerWindowSize=10

If the connection factory is directly instantiated, the producer window
size can be set via the
`ActiveMQConnectionFactory.setProducerWindowSize(int
                  producerWindowSize)` method.

#### Blocking producer window based flow control using CORE protocol

When using the CORE protocol (used by both the Artemis Core Client and Artemis JMS Client)
the server will always aim give the same number of credits as have been requested.
However, it is also possible to set a maximum size on any address, and the server
will never send more credits to any one producer than what is available according to
the address's upper memory limit.  Although a single producer will be issued more
credits than available (at the time of issue) it is possible that more than 1
producer be associated with the same address and so it is theoretically possible
that more credits are allocated across total producers than what is available.
It is therefore possible to go over the address limit by approximately:

 '''total number of producers on address * producer window size'''

For example, if I have a JMS queue called "myqueue", I could set the
maximum memory size to 10MiB, and the the server will control the number
of credits sent to any producers which are sending any messages to
myqueue such that the total messages in the queue never exceeds 10MiB.

When the address gets full, producers will block on the client side
until more space frees up on the address, i.e. until messages are
consumed from the queue thus freeing up space for more messages to be
sent.

We call this blocking producer flow control, and it's an efficient way
to prevent the server running out of memory due to producers sending
more messages than can be handled at any time.

It is an alternative approach to paging, which does not block producers
but instead pages messages to storage.

To configure an address with a maximum size and tell the server that you
want to block producers for this address if it becomes full, you need to
define an AddressSettings ([Configuring Queues Via Address Settings](queue-attributes.md)) block for the address and specify
`max-size-bytes` and `address-full-policy`

The address block applies to all queues registered to that address. I.e.
the total memory for all queues bound to that address will not exceed
`max-size-bytes`. In the case of JMS topics this means the *total*
memory of all subscriptions in the topic won't exceed max-size-bytes.

Here's an example:

    <address-settings>
       <address-setting match="jms.queue.exampleQueue">
          <max-size-bytes>100000</max-size-bytes>
          <address-full-policy>BLOCK</address-full-policy>
       </address-setting>
    </address-settings>

The above example would set the max size of the JMS queue "exampleQueue"
to be 100000 bytes and would block any producers sending to that address
to prevent that max size being exceeded.

Note the policy must be set to `BLOCK` to enable blocking producer flow
control.

> **Note**
>
> Note that in the default configuration all addresses are set to block
> producers after 10 MiB of message data is in the address. This means
> you cannot send more than 10MiB of message data to an address without
> it being consumed before the producers will be blocked. If you do not
> want this behaviour increase the `max-size-bytes` parameter or change
> the address full message policy.

> **Note**
>
> Producer credits are allocated from the broker to the client.  Flow control
> credit checking (i.e. checking a producer has enough credit) is done on the
> client side only.  It is possible for the broker to over allocate credits, like
> in the multiple producer scenario outlined above.  It is also possible for
> a misbehaving client to ignore the flow control credits issued by the broker
> and continue sending with out sufficient credit.

#### Blocking producer window based flow control using AMQP

Apache ActiveMQ Artemis ships with out of the box with 2 protocols that support flow control. Artemis CORE protocol and
AMQP. Both protocols implement flow control slightly differently and therefore address full BLOCK policy behaves slightly
different for clients that use each protocol respectively.

As explained earlier in this chapter the CORE protocol uses a producer window size flow control system. Where credits
(representing bytes) are allocated to producers, if a producer wants to send a message it should wait until it has
enough byte credits available for it to send. AMQP flow control credits are not representative of bytes but instead
represent the number of messages a producer is permitted to send (regardless of the message size).

BLOCK for AMQP works mostly in the same way as the producer window size mechanism above. Artemis will issue 100 credits
to a client at a time and refresh them when the clients credits reaches 30. The broker will stop issuing credits once an
address is full. However, since AMQP credits represent whole messages and not bytes, it would be possible in some
scenarios for an AMQP client to significantly exceed an address upper bound should the broker continue accepting
messages until the clients credits are exhausted. For this reason there is an additional parameter available on address
settings that specifies an upper bound on an address size in bytes. Once this upper bound is reach Artemis will start
rejecting AMQP messages. This limit is the max-size-bytes-reject-threshold and is by default set to -1 (or no limit).
This is additional parameter allows a kind of soft and hard limit, in normal circumstances the broker will utilize the
max-size-bytes parameter using using flow control to put back pressure on the client, but will protect the broker by
rejecting messages once the address size is reached.

### Rate limited flow control

Apache ActiveMQ Artemis also allows the rate a producer can emit message to be limited,
in units of messages per second. By specifying such a rate, Apache ActiveMQ Artemis
will ensure that producer never produces messages at a rate higher than
that specified.

The rate must be a positive integer to enable this functionality and is
the maximum desired message consumption rate specified in units of
messages per second. Setting this to `-1` disables rate limited flow
control. The default value is `-1`.

Please see [the examples chapter](examples.md) for a working example of limiting producer rate.

#### Using Core API

If the Apache ActiveMQ Artemis core API is being used the rate can be set via the
`ServerLocator.setProducerMaxRate(int producerMaxRate)` method or
alternatively via some of the `ClientSession.createProducer()` methods.

#### Using JMS

If JNDI is used to instantiate and look up the connection factory, the
max rate size can be configured in the JNDI context environment, e.g.
`jndi.properties`. Here's a simple example using the "ConnectionFactory"
connection factory which is available in the context by default:

    java.naming.factory.initial=org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory
    connectionFactory.myConnectionFactory=tcp://localhost:61616?producerMaxRate=10

If the connection factory is directly instantiated, the max rate size
can be set via the `ActiveMQConnectionFactory.setProducerMaxRate(int
                  producerMaxRate)` method.
