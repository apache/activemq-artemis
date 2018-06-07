# Using Core

Apache ActiveMQ Artemis core is a messaging system with its own API. We call
this the *core API*.

If you don't want to use the JMS API or any of the other supported protocols
you can use the core API directly. The core API provides all the functionality
of JMS but without much of the complexity. It also provides features that are
not available using JMS.

## Core Messaging Concepts

Some of the core messaging concepts are similar to JMS concepts, but core
messaging concepts are also different in some ways as well. In general the core
API is simpler than the JMS API, since we remove distinctions between queues,
topics and subscriptions. We'll discuss each of the major core messaging
concepts in turn, but to see the API in detail please consult the Javadoc.

Also refer to the [addressing model](address-model.md) chapter for a high-level
overview of these concepts as well as configuration details.

### Message

- A message is the unit of data which is sent between clients and servers.

- A message has a body which is a buffer containing convenient methods for
  reading and writing data into it.

- A message has a set of properties which are key-value pairs. Each property
  key is a string and property values can be of type integer, long, short,
  byte, byte[], String, double, float or boolean.

- A message has an *address* it is being sent to. When the message arrives on
  the server it is routed to any queues that are bound to the address. The
  routing semantics (i.e. anycast or multicast) are determined by the "routing
  type" of the address and queue. If the queues are bound with any filter, the
  message will only be routed to that queue if the filter matches. An address may
  have many queues bound to it or even none. There may also be entities other
  than queues (e.g. *diverts*) bound to addresses.

- Messages can be either durable or non durable. Durable messages in a durable
  queue will survive a server crash or restart. Non durable messages will never
  survive a server crash or restart.

- Messages can be specified with a priority value between 0 and 9. 0 represents
  the lowest priority and 9 represents the highest. The broker will attempt to
  deliver higher priority messages before lower priority ones.

- Messages can be specified with an optional expiry time. The broker will not
  deliver messages after its expiry time has been exceeded.

- Messages also have an optional timestamp which represents the time the
  message was sent.

- Apache ActiveMQ Artemis also supports the sending/consuming of very large
  messages much larger than can fit in available RAM at any one time.

### Address

A server maintains a mapping between an address and a set of queues. Zero or
more queues can be bound to a single address. Each queue can be bound with an
optional message filter. When a message is routed, it is routed to the set of
queues bound to the message's address. If any of the queues are bound with a
filter expression, then the message will only be routed to the subset of bound
queues which match that filter expression.

Other entities, such as *diverts* can also be bound to an address and messages
will also be routed there.

> **Note:**
>
> Although core supports publish-subscribe semantics there is no such thing as
> a "topic" per se. "Topic" is mainly a JMS term. In core we just deal with
> *addresses*, *queues*, and *routing types*.
>
> For example, a JMS topic would be implemented by a single address to which
> many queues are bound using multicast routing. Each queue represents a
> "subscription" in normal "topic" terms. A JMS queue would be implemented as a
> single address to which one queue is bound using anycast routing - that queue
> represents the JMS queue.

### Queue

Queues can be durable, meaning the messages they contain survive a server crash
or restart, as long as the messages in them are durable. Non durable queues do
not survive a server restart or crash even if the messages they contain are
durable.

Queues can also be temporary, meaning they are automatically deleted when the
client connection is closed, if they are not explicitly deleted before that.

Queues can be bound with an optional filter expression. If a filter expression
is supplied then the server will only route messages that match that filter
expression to any queues bound to the address.

Many queues can be bound to a single address. A particular queue is only bound
to a maximum of one address.

### Routing Type

The routing type determines the semantics used when routing messages to the
queues bound to the address where the message was sent. Two types are
supported:

- `ANYCAST`

  The message is routed to only **one** of the queues bound to the address. If
  multiple queues are bound to the address then messages are routed to them in a
  round-robin fashion.

- `MULTICAST`

  The message is route to **all** of the queues bound to the address.

## Core API

### ServerLocator

Clients use `ServerLocator` instances to create `ClientSessionFactory`
instances. `ServerLocator` instances are used to locate servers and create
connections to them.

In JMS terms think of a `ServerLocator` in the same way you would a JMS
Connection Factory.

`ServerLocator` instances are created using the `ActiveMQClient` factory class.

### ClientSessionFactory

Clients use `ClientSessionFactory` instances to create `ClientSession`
instances. `ClientSessionFactory` instances are basically the connection to a
server

In JMS terms think of them as JMS Connections.

`ClientSessionFactory` instances are created using the `ServerLocator` class.

### ClientSession

A client uses a `ClientSession`for consuming and producing messages and for
grouping them in transactions. `ClientSession` instances can support both
transactional and non transactional semantics and also provide an `XAResource`
interface so messaging operations can be performed as part of a
[JTA](http://www.oracle.com/technetwork/java/javaee/tech/jta-138684.html)
transaction.

`ClientSession` instances group `ClientConsumer` instances and `ClientProducer`
instances.

`ClientSession` instances can be registered with an optional
`SendAcknowledgementHandler`. This allows your client code to be notified
asynchronously when sent messages have successfully reached the server. This
unique Apache ActiveMQ Artemis feature, allows you to have full guarantees that
sent messages have reached the server without having to block on each message
sent until a response is received. Blocking on each messages sent is costly
since it requires a network round trip for each message sent. By not blocking
and receiving send acknowledgements asynchronously you can create true end to
end asynchronous systems which is not possible using the standard JMS API. For
more information on this advanced feature please see the section [Guarantees of
sends and commits](send-guarantees.md).

### ClientConsumer

Clients use `ClientConsumer` instances to consume messages from a queue. Core
messaging supports both synchronous and asynchronous message consumption
semantics. `ClientConsumer` instances can be configured with an optional filter
expression and will only consume messages which match that expression.

### ClientProducer

Clients create `ClientProducer` instances on `ClientSession` instances so they
can send messages. `ClientProducer` instances can specify an address to which
all sent messages are routed, or they can have no specified address, and the
address is specified at send time for the message.

> **Warning**
>
> Please note that `ClientSession`, `ClientProducer` and `ClientConsumer`
> instances are *designed to be re-used*.
>
> It's an anti-pattern to create new `ClientSession`, `ClientProducer` and
> `ClientConsumer` instances for each message you produce or consume. If you do
> this, your application will perform very poorly. This is  discussed further
> in the section on performance tuning [Performance Tuning](perf-tuning.md).

## A simple example of using Core

Here's a very simple program using the core messaging API to send and receive a
message. Logically it's comprised of two sections: firstly setting up the
producer to write a message to an *addresss*, and secondly, creating a *queue*
for the consumer using anycast routing, creating the consumer, and *starting*
it.

```java
ServerLocator locator = ActiveMQClient.createServerLocator("vm://0");

// In this simple example, we just use one session for both producing and receiving

ClientSessionFactory factory =  locator.createClientSessionFactory();
ClientSession session = factory.createSession();

// A producer is associated with an address ...

ClientProducer producer = session.createProducer("example");
ClientMessage message = session.createMessage(true);
message.getBodyBuffer().writeString("Hello");

// We need a queue attached to the address ...

session.createQueue("example", RoutingType.ANYCAST, "example", true);

// And a consumer attached to the queue ...

ClientConsumer consumer = session.createConsumer("example");

// Once we have a queue, we can send the message ...

producer.send(message);

// We need to start the session before we can -receive- messages ...

session.start();
ClientMessage msgReceived = consumer.receive();

System.out.println("message = " + msgReceived.getBodyBuffer().readString());

session.close();
```
