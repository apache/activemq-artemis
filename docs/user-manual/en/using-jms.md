# Using JMS

Although Apache ActiveMQ Artemis provides a JMS agnostic messaging API, many users will
be more comfortable using JMS.

JMS is a very popular API standard for messaging, and most messaging
systems provide a JMS API. If you are completely new to JMS we suggest
you follow the [Oracle JMS tutorial](http://docs.oracle.com/javaee/7/tutorial/partmessaging.htm) -
a full JMS tutorial is out of scope for this guide.

Apache ActiveMQ Artemis also ships with a wide range of examples, many of which
demonstrate JMS API usage. A good place to start would be to play around
with the simple JMS Queue and Topic example, but we also provide
examples for many other parts of the JMS API. A full description of the
examples is available in [Examples](examples.md).

In this section we'll go through the main steps in configuring the
server for JMS and creating a simple JMS program. We'll also show how to
configure and use JNDI, and also how to use JMS with Apache ActiveMQ Artemis without
using any JNDI.

A simple ordering system
========================

For this chapter we're going to use a very simple ordering system as our
example. It is a somewhat contrived example because of its extreme
simplicity, but it serves to demonstrate the very basics of setting up
and using JMS.

We will have a single JMS Queue called `OrderQueue`, and we will have a
single `MessageProducer` sending an order message to the queue and a
single `MessageConsumer` consuming the order message from the queue.

The queue will be a `durable` queue, i.e. it will survive a server
restart or crash. We also want to pre-deploy the queue, i.e. specify the
queue in the server configuration so it is created automatically
without us having to explicitly create it from the client.

JNDI Configuration
==================

The JMS specification establishes the convention that *administered
objects* (i.e. JMS queue, topic and connection factory instances) are
made available via the JNDI API. Brokers are free to implement JNDI as
they see fit assuming the implementation fits the API. Apache ActiveMQ Artemis does not
have a JNDI server. Rather, it uses a client-side JNDI implementation
that relies on special properties set in the environment to construct
the appropriate JMS objects. In other words, no objects are stored in
JNDI on the Apache ActiveMQ Artemis server, instead they are simply instantiated on the
client based on the provided configuration. Let's look at the different
kinds of administered objects and how to configure them.

> **Note**
>
> The following configuration properties *are strictly required when
> Apache ActiveMQ Artemis is running in stand-alone mode*. When Apache ActiveMQ Artemis is integrated
> to an application server (e.g. Wildfly) the application server itself
> will almost certainly provide a JNDI client with its own properties.

ConnectionFactory JNDI
----------------------

A JMS connection factory is used by the client to make connections to
the server. It knows the location of the server it is connecting to, as
well as many other configuration parameters.

Here's a simple example of the JNDI context environment for a client
looking up a connection factory to access an *embedded* instance of
Apache ActiveMQ Artemis:

    java.naming.factory.initial=org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory
    connectionFactory.invmConnectionFactory=vm://0

In this instance we have created a connection factory that is bound to
`invmConnectionFactory`, any entry with prefix `connectionFactory.` will
  create a connection factory.

In certain situations there could be multiple server instances running
within a particular JVM. In that situation each server would typically
have an InVM acceptor with a unique server-ID. A client using JMS and
JNDI can account for this by specifying a connction factory for each
server, like so:

    java.naming.factory.initial=org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory
    connectionFactory.invmConnectionFactory0=vm://0
    connectionFactory.invmConnectionFactory1=vm://1
    connectionFactory.invmConnectionFactory2=vm://2

Here is a list of all the supported URL schemes:

-   `vm`

-   `tcp`

-   `udp`

-   `jgroups`

Most clients won't be connecting to an embedded broker. Clients will
most commonly connect across a network a remote broker. Here's a simple
example of a client configuring a connection factory to connect to a
remote broker running on myhost:5445:

    java.naming.factory.initial=org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory
    connectionFactory.ConnectionFactory=tcp://myhost:5445

In the example above the client is using the `tcp` scheme for the
provider URL. A client may also specify multiple comma-delimited
host:port combinations in the URL (e.g.
`(tcp://remote-host1:5445,remote-host2:5445)`). Whether there is one or
many host:port combinations in the URL they are treated as the *initial
connector(s)* for the underlying connection.

The `udp` scheme is also supported which should use a host:port
combination that matches the `group-address` and `group-port` from the
corresponding `broadcast-group` configured on the ActiveMQ Artemis server(s).

Each scheme has a specific set of properties which can be set using the
traditional URL query string format (e.g.
`scheme://host:port?key1=value1&key2=value2`) to customize the
underlying transport mechanism. For example, if a client wanted to
connect to a remote server using TCP and SSL it would create a connection
factory like so, `tcp://remote-host:5445?ssl-enabled=true`.

All the properties available for the `tcp` scheme are described in [the
documentation regarding the Netty
transport](configuring-transports.md#configuring-the-netty-transport).

Note if you are using the `tcp` scheme and multiple addresses then a query
can be applied to all the url's or just to an individual connector, so where
you have

-   `(tcp://remote-host1:5445?httpEnabled=true,remote-host2:5445?httpEnabled=true)?clientID=1234`

then the `httpEnabled` property is only set on the individual connectors where as the `clientId`
is set on the actual connection factory. Any connector specific properties set on the whole
URI will be applied to all the connectors.


The `udp` scheme supports 4 properties:

-   `localAddress` - If you are running with multiple network
    interfaces on the same machine, you may want to specify that the
    discovery group listens only only a specific interface. To do this
    you can specify the interface address with this parameter.

-   `localPort` - If you want to specify a local port to which the
    datagram socket is bound you can specify it here. Normally you would
    just use the default value of -1 which signifies that an anonymous
    port should be used. This parameter is always specified in
    conjunction with `localAddress`.

-   `refreshTimeout` - This is the period the discovery group waits
    after receiving the last broadcast from a particular server before
    removing that servers connector pair entry from its list. You would
    normally set this to a value significantly higher than the
    broadcast-period on the broadcast group otherwise servers might
    intermittently disappear from the list even though they are still
    broadcasting due to slight differences in timing. This parameter is
    optional, the default value is 10000 milliseconds (10 seconds).

-   `discoveryInitialWaitTimeout` - If the connection factory is used
    immediately after creation then it may not have had enough time to
    received broadcasts from all the nodes in the cluster. On first
    usage, the connection factory will make sure it waits this long
    since creation before creating the first connection. The default
    value for this parameter is 10000 milliseconds.

Lastly, the `jgroups` scheme is supported which provides an alternative
to the `udp` scheme for server discovery. The URL pattern is either
`jgroups://channelName?file=jgroups-xml-conf-filename`
where`jgroups-xml-conf-filename` refers to an XML file on the classpath
that contains the JGroups configuration or it can be
`jgroups://channelName?properties=some-jgroups-properties`. In both instance the
`channelName` is the name given to the jgroups channel created.

The `refreshTimeout` and `discoveryInitialWaitTimeout` properties
are supported just like with `udp`.

The default type for the default connection factory is of type `javax.jms.ConnectionFactory`.
This can be changed by setting the type like so

    java.naming.factory.initial=org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory
    java.naming.provider.url=tcp://localhost:5445?type=CF

In this example it is still set to the default, below shows a list of types that can be set.

#### Configuration for Connection Factory Types

type | interface
--- |---
CF (default) | javax.jms.ConnectionFactory
XA_CF | javax.jms.XAConnectionFactory
QUEUE_CF | javax.jms.QueueConnectionFactory
QUEUE_XA_CF | javax.jms.XAQueueConnectionFactory
TOPIC_CF | javax.jms.TopicConnectionFactory
TOPIC_XA_CF | javax.jms.XATopicConnectionFactory

### Destination JNDI

JMS destinations are also typically looked up via JNDI. As with
connection factories, destinations can be configured using special
properties in the JNDI context environment. The property *name* should
follow the pattern: `queue.<jndi-binding>` or `topic.<jndi-binding>`.
The property *value* should be the name of the queue hosted by the
Apache ActiveMQ Artemis server. For example, if the server had a JMS queue configured
like so:

    <queue name="OrderQueue"/>

And if the client wanted to bind this queue to "queues/OrderQueue" then
the JNDI properties would be configured like so:

    java.naming.factory.initial=org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory
    java.naming.provider.url=tcp://myhost:5445
    queue.queues/OrderQueue=OrderQueue

It is also possible to look-up JMS destinations which haven't been
configured explicitly in the JNDI context environment. This is possible
using `dynamicQueues/` or `dynamicTopics/` in the look-up string. For
example, if the client wanted to look-up the aforementioned "OrderQueue"
it could do so simply by using the string "dynamicQueues/OrderQueue".
Note, the text that follows `dynamicQueues/` or `dynamicTopics/` must
correspond *exactly* to the name of the destination on the server.

### The code

Here's the code for the example:

First we'll create a JNDI initial context from which to lookup our JMS
objects. If the above properties are set in `jndi.properties` and it is
on the classpath then any new, empty `InitialContext` will be
initialized using those properties:
``` java
InitialContext ic = new InitialContext();

//Now we'll look up the connection factory from which we can create
//connections to myhost:5445:

ConnectionFactory cf = (ConnectionFactory)ic.lookup("ConnectionFactory");

//And look up the Queue:

Queue orderQueue = (Queue)ic.lookup("queues/OrderQueue");

//Next we create a JMS connection using the connection factory:

Connection connection = cf.createConnection();

//And we create a non transacted JMS Session, with AUTO\_ACKNOWLEDGE
//acknowledge mode:

Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

//We create a MessageProducer that will send orders to the queue:

MessageProducer producer = session.createProducer(orderQueue);

//And we create a MessageConsumer which will consume orders from the
//queue:

MessageConsumer consumer = session.createConsumer(orderQueue);

//We make sure we start the connection, or delivery won't occur on it:

connection.start();

//We create a simple TextMessage and send it:

TextMessage message = session.createTextMessage("This is an order");
producer.send(message);

//And we consume the message:

TextMessage receivedMessage = (TextMessage)consumer.receive();
System.out.println("Got order: " + receivedMessage.getText());
```

It is as simple as that. For a wide range of working JMS examples please
see the examples directory in the distribution.

> **Warning**
>
> Please note that JMS connections, sessions, producers and consumers
> are *designed to be re-used*.
>
> It is an anti-pattern to create new connections, sessions, producers
> and consumers for each message you produce or consume. If you do this,
> your application will perform very poorly. This is discussed further
> in the section on performance tuning [Performance Tuning](perf-tuning.md).

### Directly instantiating JMS Resources without using JNDI

Although it is a very common JMS usage pattern to lookup JMS
*Administered Objects* (that's JMS Queue, Topic and ConnectionFactory
instances) from JNDI, in some cases you just think "Why do I need JNDI?
Why can't I just instantiate these objects directly?"

With Apache ActiveMQ Artemis you can do exactly that. Apache ActiveMQ Artemis supports the direct
instantiation of JMS Queue, Topic and ConnectionFactory instances, so
you don't have to use JNDI at all.

>For a full working example of direct instantiation please look at the
>"Instantiate JMS Objects Directly" example under the JMS section of the
>examples.  See the [Examples](examples.md) section for more info.

Here's our simple example, rewritten to not use JNDI at all:

We create the JMS ConnectionFactory object via the ActiveMQJMSClient
Utility class, note we need to provide connection parameters and specify
which transport we are using, for more information on connectors please
see [Configuring the Transport](configuring-transports.md).

``` java
TransportConfiguration transportConfiguration = new TransportConfiguration(NettyConnectorFactory.class.getName());

ConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithoutHA(JMSFactoryType.CF,transportConfiguration);

//We also create the JMS Queue object via the ActiveMQJMSClient Utility
//class:

Queue orderQueue = ActiveMQJMSClient.createQueue("OrderQueue");

//Next we create a JMS connection using the connection factory:

Connection connection = cf.createConnection();

//And we create a non transacted JMS Session, with AUTO\_ACKNOWLEDGE
//acknowledge mode:

Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

//We create a MessageProducer that will send orders to the queue:

MessageProducer producer = session.createProducer(orderQueue);

//And we create a MessageConsumer which will consume orders from the
//queue:

MessageConsumer consumer = session.createConsumer(orderQueue);

//We make sure we start the connection, or delivery won't occur on it:

connection.start();

//We create a simple TextMessage and send it:

TextMessage message = session.createTextMessage("This is an order");
producer.send(message);

//And we consume the message:

TextMessage receivedMessage = (TextMessage)consumer.receive();
System.out.println("Got order: " + receivedMessage.getText());
```

### Setting The Client ID

This represents the client id for a JMS client and is needed for
creating durable subscriptions. It is possible to configure this on the
connection factory and can be set via the `clientId` element. Any
connection created by this connection factory will have this set as its
client id.

### Setting The Batch Size for DUPS_OK

When the JMS acknowledge mode is set to `DUPS_OK` it is possible to
configure the consumer so that it sends acknowledgements in batches
rather that one at a time, saving valuable bandwidth. This can be
configured via the connection factory via the `dupsOkBatchSize`
element and is set in bytes. The default is 1024 \* 1024 bytes = 1 MiB.

### Setting The Transaction Batch Size

When receiving messages in a transaction it is possible to configure the
consumer to send acknowledgements in batches rather than individually
saving valuable bandwidth. This can be configured on the connection
factory via the `transactionBatchSize` element and is set in bytes.
The default is 1024 \* 1024.

### Setting The Destination Cache

Many frameworks such as Spring resolve the destination by name on every operation,
this can cause a performance issue and extra calls to the broker, 
in a scenario where destinations (addresses) are permanent broker side, 
such as they are managed by a platform or operations team.
using `destinationCache` element, you can toggle on the destination cache 
to improve the performance and reduce the calls to the broker.
This should not be used if destinations (addresses) are not permanent broker side, 
as in dynamic creation/deletion.
