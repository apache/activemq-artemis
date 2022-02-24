# Last-Value Queues

Last-Value queues are special queues which discard any messages when a newer
message with the same value for a well-defined Last-Value property is put in
the queue. In other words, a Last-Value queue only retains the last value.

A typical example for Last-Value queue is for stock prices, where you are only
interested by the latest value for a particular stock.

Messages sent to an Last-Value queue without the specified property will be
delivered as normal and will never be "replaced".

## Configuration

#### Last Value Key Configuration
Last-Value queues can be statically configured in broker.xml via the
`last-value-key`

```xml
<address name="foo.bar">
   <multicast>
      <queue name="orders1" last-value-key="reuters_code" />
   </multicast>
</address>
```

Specified on creating a queue by using the CORE api specifying the parameter 
`lastValue` to `true`. 

Or on auto-create when using the JMS Client by using address parameters when 
creating the destination used by the consumer.

```java
Queue queue = session.createQueue("my.destination.name?last-value-key=reuters_code");
Topic topic = session.createTopic("my.destination.name?last-value-key=reuters_code");
```

Address wildcards can be used to configure Last-Value queues for a set of
addresses (see [here](wildcard-syntax.md)).

```xml
<address-setting match="lastValueQueue">
   <default-last-value-key>reuters_code</default-last-value-key>
</address-setting>
```

By default, `default-last-value-key` is `null`.

#### Legacy Last Value Configuration

Last-Value queues can also just be configured via the `last-value` boolean
property, doing so it will default the last-value-key to `_AMQ_LVQ_NAME`.

```xml
<address name="foo.bar">
   <multicast>
      <queue name="orders1" last-value="true" />
   </multicast>
</address>
```

Specified on creating a queue by using the CORE api specifying the parameter 
`lastValue` to `true`. 

Or on auto-create when using the JMS Client by using address parameters when 
creating the destination used by the consumer.

```java
Queue queue = session.createQueue("my.destination.name?last-value=true");
Topic topic = session.createTopic("my.destination.name?last-value=true");
```

Also the default for all queues under and address can be defaulted using the 
`address-setting` configuration:

```xml
<address-setting match="lastValueQueue">
   <default-last-value-queue>true</default-last-value-queue>
</address-setting>
```

By default, `default-last-value-queue` is false. 

Note that `address-setting` `last-value-queue` config is deprecated, please use
`default-last-value-queue` instead.

## Last-Value Property

The property name used to identify the last value is configurable 
at the queue level mentioned above.

If using the legacy setting to configure an LVQ then the default property 
`"_AMQ_LVQ_NAME"` is used (or the constant `Message.HDR_LAST_VALUE_NAME` from
the Core API).

For example, using the sample configuration 

```xml
<address name="foo.bar">
   <multicast>
      <queue name="orders1" last-value-key="reuters_code" />
   </multicast>
</address>
```

if two messages with the same value for the Last-Value
property are sent to a Last-Value queue, only the latest message will be
kept in the queue:

```java
// send 1st message with Last-Value property `reuters_code` set to `VOD`
TextMessage message = session.createTextMessage("1st message with Last-Value property set");
message.setStringProperty("reuters_code", "VOD");
producer.send(message);

// send 2nd message with Last-Value property `reuters_code` set to `VOD`
message = session.createTextMessage("2nd message with Last-Value property set");
message.setStringProperty("reuters_code", "VOD");
producer.send(message);

...

// only the 2nd message will be received: it is the latest with
// the Last-Value property set
TextMessage messageReceived = (TextMessage)messageConsumer.receive(5000);
System.out.format("Received message: %s\n", messageReceived.getText());
```

## Forcing all consumers to be non-destructive

It's common to combine last-value queues with [non-destructive](non-destructive-queues.md)
semantics.

## Clustering

The fundamental ideas behind last-value queues and clustering are at odds with
each other. 

Clustering was designed as a way to increase message throughput through
horizontal scaling. The messages in a clustered queue can be spread across 
_all_ nodes in the cluster. This allows clients to be distributed across the
cluster to leverage the computing resources all the nodes rather than being
bottlenecked on a single node.

However, if you wanted to use a last-value queue in a cluster then in order
to enforce last-value semantics all messages would be required to go to a
queue on a _single_ node. This would effectively _nullify_ the benefits of
clustering. Also, the arrival of messages on and and redistribution of
those messages from nodes other than the node where the last-value semantics
would be enforced would almost certainly impact which message is considered
"last."

For these reasons last-value queues are not supported in a traditional cluster.
However, it would be possible to use a [broker balancer](broker-balancers.md)
in front of a cluster (or even a set of non-clustered brokers) to ensure all
clients which need to use the same last-value queue are directed to the same
node. See the [broker balancer chapter](broker-balancers.md) for more details
on configuration, etc.

## Example

See the [last-value queue example](examples.md#last-value-queue) which shows 
how last value queues are configured and used with JMS.
