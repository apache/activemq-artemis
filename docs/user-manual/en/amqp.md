# AMQP

Apache ActiveMQ Artemis supports the [AMQP
1.0](https://www.oasis-open.org/committees/tc_home.php?wg_abbrev=amqp)
specification. By default there are `acceptor` elements configured to accept
AMQP connections on ports `61616` and `5672`.

See the general [Protocols and Interoperability](protocols-interoperability.md)
chapter for details on configuring an `acceptor` for AMQP.

You can use *any* AMQP 1.0 compatible clients.

A short list includes:

- [qpid clients](https://qpid.apache.org/download.html)
- [.NET Clients](https://blogs.apache.org/activemq/entry/using-net-libraries-with-activemq)
- [Javascript NodeJS](https://github.com/noodlefrenzy/node-amqp10)
- [Java Script RHEA](https://github.com/grs/rhea)
- ... and many others.

## Examples

We have a few examples as part of the Artemis distribution:

- .NET: 
  - ./examples/protocols/amqp/dotnet
- ProtonCPP
  - ./examples/protocols/amqp/proton-cpp
  - ./examples/protocols/amqp/proton-clustered-cpp
- Ruby
  - ./examples/protocols/amqp/proton-ruby
- Java (Using the qpid JMS Client)
  - ./examples/protocols/amqp/queue
- Interceptors
  - ./examples/features/standard/interceptor-amqp
  - ./examples/features/standard/broker-plugin

## Message Conversions

The broker will not perform any message conversion to any other protocols when
sending AMQP and receiving AMQP.

However if you intend your message to be received by an AMQP JMS Client, you
must follow the [JMS Mapping
Conventions](https://www.oasis-open.org/committees/download.php/53086/amqp-bindmap-jms-v1.0-wd05.pdf).
If you send a body type that is not recognized by this specification the
conversion between AMQP and any other protocol will make it a Binary Message.
Make sure you follow these conventions if you intend to cross protocols or
languages.  Especially on the message body.

A compatibility setting allows aligning the naming convention of AMQP queues
(JMS Durable and Shared Subscriptions) with CORE. For backwards compatibility
reasons, you need to explicitly enable this via broker configuration:

- `amqp-use-core-subscription-naming`
   - `true` - use queue naming convention that is aligned with CORE.
   - `false` (default) - use older naming convention.   

## Intercepting and changing messages
 
We don't recommend changing messages at the server's side for a few reasons:
 
- AMQP messages are meant to be immutable
- The message won't be the original message the user sent
- AMQP has the possibility of signing messages. The signature would be broken.
- For performance reasons. We try not to re-encode (or even decode) messages.

If regardless these recommendations you still need and want to intercept and
change AMQP messages, look at the aforementioned interceptor examples.

## AMQP and security

The Apache ActiveMQ Artemis Server accepts the PLAIN, ANONYMOUS, and GSSAPI
SASL mechanism. These are implemented on the broker's [security](security.md)
infrastructure.

## AMQP and destinations

If an AMQP Link is dynamic then a temporary queue will be created and either
the remote source or remote target address will be set to the name of the
temporary queue. If the Link is not dynamic then the address of the remote 
target or source will be used for the queue. In case it does not exist, 
it will be auto-created if the settings allow.

## AMQP and Multicast Addresses (Topics)

Although AMQP has no notion of "topics" it is still possible to treat AMQP
consumers or receivers as subscriptions rather than just consumers on a queue.
By default any receiving link that attaches to an address that has only
`multicast` enabled will be treated as a subscription and a corresponding
subscription queue will be created. If the Terminus Durability is either
`UNSETTLED_STATE` or `CONFIGURATION` then the queue will be made durable
(similar to a JMS durable subscription) and given a name made up from the
container id and the link name, something like `my-container-id:my-link-name`.
If the Terminus Durability is configured as `NONE` then a volatile `multicast`
queue will be created.

## AMQP and Coordinations - Handling Transactions

An AMQP links target can also be a Coordinator. A Coordinator is used to handle
transactions. If a coordinator is used then the underlying server session will
be transacted and will be either rolled back or committed via the coordinator.

> **Note:**
>
> AMQP allows the use of multiple transactions per session,
> `amqp:multi-txns-per-ssn`, however in this version of Apache ActiveMQ Artemis
> will only support single transactions per session.

## AMQP scheduling message delivery

An AMQP message can provide scheduling information that controls the time in
the future when the message will be delivered at the earliest.  This
information is provided by adding a message annotation to the sent message.

There are two different message annotations that can be used to schedule a
message for later delivery:

- `x-opt-delivery-time`
  The specified value must be a positive long corresponding to the time the
  message should be made available for delivery (in milliseconds).

- `x-opt-delivery-delay`
  The specified value must be a positive long corresponding to the amount of
  milliseconds after the broker receives the given message before it should be
  made available for delivery.

If both annotations are present in the same message then the broker will prefer
the more specific `x-opt-delivery-time` value.

## DLQ and Expiry transfer

AMQP Messages will be copied before transferred to a DLQ or ExpiryQueue and will receive properties and annotations during this process.

The broker also keeps an internal only property (called extra property) that is not exposed to the clients, and those will also be filled during this process.

Here is a list of Annotations and Property names AMQP Messages will receive when transferred:

|Annotation name| Internal Property Name|Description|
|---------------|-----------------------|-----------|
|x-opt-ORIG-MESSAGE-ID|_AMQ_ORIG_MESSAGE_ID|The original message ID before the transfer|
|x-opt-ACTUAL-EXPIRY|_AMQ_ACTUAL_EXPIRY|When the expiry took place. Milliseconds since epoch times|
|x-opt-ORIG-QUEUE|_AMQ_ORIG_QUEUE|The original queue name before the transfer|
|x-opt-ORIG-ADDRESS|_AMQ_ORIG_ADDRESS|The original address name before the transfer|

## Filtering on Message Annotations

It is possible to filter on messaging annotations if you use the prefix "m." before the annotation name.

For example if you want to filter messages sent to a specific destination, you could create your filter accordingly to this:

```java
ConnectionFactory factory = new JmsConnectionFactory("amqp://localhost:5672");
Connection connection = factory.createConnection();
Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
connection.start();
javax.jms.Queue queue = session.createQueue("my-DLQ");
MessageConsumer consumer = session.createConsumer(queue, "\"m.x-opt-ORIG-ADDRESS\"='ORIGINAL_PLACE'");
Message message = consumer.receive();
```

The broker will set internal properties. If you intend to filter after DLQ or Expiry you may choose the internal property names:

```java
// Replace the consumer creation on the previous example:
MessageConsumer consumer = session.createConsumer(queue, "_AMQ_ORIG_ADDRESS='ORIGINAL_PLACE'");
```


## Configuring AMQP Idle Timeout

It is possible to configure the AMQP Server's IDLE Timeout by setting the property amqpIdleTimeout in milliseconds on the acceptor.

This will make the server to send an AMQP frame open to the client, with your configured timeout / 2.

So, if you configured your AMQP Idle Timeout to be 60000, the server will tell the client to send frames every 30,000 milliseconds.


```xml
<acceptor name="amqp">.... ;amqpIdleTimeout=<configured-timeout>; ..... </acceptor>
```


### Disabling Keep alive checks

if you set amqpIdleTimeout=0 that will tell clients to not sending keep alive packets towards the server. On this case
you will rely on TCP to determine when the socket needs to be closed.

```xml
<acceptor name="amqp">.... ;amqpIdleTimeout=0; ..... </acceptor>
```

This contains a real example for configuring amqpIdleTimeout:

```xml
<acceptor name="amqp">tcp://0.0.0.0:5672?amqpIdleTimeout=0;tcpSendBufferSize=1048576;tcpReceiveBufferSize=1048576;protocols=AMQP;useEpoll=true;amqpCredits=1000;amqpMinCredits=300;directDeliver=false;batchDelay=10</acceptor>
```

## Web Sockets

Apache ActiveMQ Artemis also supports AMQP over [Web
Sockets](https://html.spec.whatwg.org/multipage/web-sockets.html).  Modern web
browsers which support Web Sockets can send and receive AMQP messages.

AMQP over Web Sockets is supported via a normal AMQP acceptor:

```xml
<acceptor name="amqp-ws-acceptor">tcp://localhost:5672?protocols=AMQP</acceptor>
```

With this configuration, Apache ActiveMQ Artemis will accept AMQP connections
over Web Sockets on the port `5672`. Web browsers can then connect to
`ws://<server>:5672` using a Web Socket to send and receive AMQP messages.