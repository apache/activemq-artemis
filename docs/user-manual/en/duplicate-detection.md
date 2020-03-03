# Duplicate Message Detection

Apache ActiveMQ Artemis includes powerful automatic duplicate message detection,
filtering out duplicate messages without you having to code your own
fiddly duplicate detection logic at the application level. This chapter
will explain what duplicate detection is, how Apache ActiveMQ Artemis uses it and how
and where to configure it.

When sending messages from a client to a server, or indeed from a server
to another server, if the target server or connection fails sometime
after sending the message, but before the sender receives a response
that the send (or commit) was processed successfully then the sender
cannot know for sure if the message was sent successfully to the
address.

If the target server or connection failed after the send was received
and processed but before the response was sent back then the message
will have been sent to the address successfully, but if the target
server or connection failed before the send was received and finished
processing then it will not have been sent to the address successfully.
From the senders point of view it's not possible to distinguish these
two cases.

When the server recovers this leaves the client in a difficult
situation. It knows the target server failed, but it does not know if
the last message reached its destination ok. If it decides to resend the
last message, then that could result in a duplicate message being sent
to the address. If each message was an order or a trade then this could
result in the order being fulfilled twice or the trade being double
booked. This is clearly not a desirable situation.

Sending the message(s) in a transaction does not help out either. If the
server or connection fails while the transaction commit is being
processed it is also indeterminate whether the transaction was
successfully committed or not!

To solve these issues Apache ActiveMQ Artemis provides automatic duplicate messages
detection for messages sent to addresses.

## Using Duplicate Detection for Message Sending

Enabling duplicate message detection for sent messages is simple: you
just need to set a special property on the message to a unique value.
You can create the value however you like, as long as it is unique. When
the target server receives the message it will check if that property is
set, if it is, then it will check in its in memory cache if it has
already received a message with that value of the header. If it has
received a message with the same value before then it will ignore the
message.

> **Note:**
>
> Using duplicate detection to move messages between nodes can give you
> the same *once and only once* delivery guarantees as if you were using
> an XA transaction to consume messages from source and send them to the
> target, but with less overhead and much easier configuration than
> using XA.

If you're sending messages in a transaction then you don't have to set
the property for *every* message you send in that transaction, you only
need to set it once in the transaction. If the server detects a
duplicate message for any message in the transaction, then it will
ignore the entire transaction.

The name of the property that you set is given by the value of
`org.apache.activemq.artemis.api.core.Message.HDR_DUPLICATE_DETECTION_ID`, which
is `_AMQ_DUPL_ID`

The value of the property can be of type `byte[]` or `SimpleString` if
you're using the core API. If you're using JMS it must be a `String`,
and its value should be unique. An easy way of generating a unique id is
by generating a UUID.

Here's an example of setting the property using the core API:

```java
...

ClientMessage message = session.createMessage(true);

SimpleString myUniqueID = "This is my unique id";   // Could use a UUID for this

message.setStringProperty(HDR_DUPLICATE_DETECTION_ID, myUniqueID);

```

And here's an example using the JMS API:

```java
...

Message jmsMessage = session.createMessage();

String myUniqueID = "This is my unique id";   // Could use a UUID for this

message.setStringProperty(HDR_DUPLICATE_DETECTION_ID.toString(), myUniqueID);

...
```

## Configuring the Duplicate ID Cache

The server maintains caches of received values of the
`org.apache.activemq.artemis.core.message.impl.HDR_DUPLICATE_DETECTION_ID`
property sent to each address. Each address has its own distinct cache.

The cache is a circular fixed size cache. If the cache has a maximum
size of `n` elements, then the `n + 1`th id stored will overwrite the
`0`th element in the cache.

The maximum size of the cache is configured by the parameter
`id-cache-size` in `broker.xml`, the default value is
`20000` elements.

The caches can also be configured to persist to disk or not. This is
configured by the parameter `persist-id-cache`, also in
`broker.xml`. If this is set to `true` then each id will
be persisted to permanent storage as they are received. The default
value for this parameter is `true`.

> **Note:**
>
> When choosing a size of the duplicate id cache be sure to set it to a
> larger enough size so if you resend messages all the previously sent
> ones are in the cache not having been overwritten.

## Duplicate Detection and Bridges

Core bridges can be configured to automatically add a unique duplicate
id value (if there isn't already one in the message) before forwarding
the message to its target. This ensures that if the target server
crashes or the connection is interrupted and the bridge resends the
message, then if it has already been received by the target server, it
will be ignored.

To configure a core bridge to add the duplicate id header, simply set
the `use-duplicate-detection` to `true` when configuring a bridge in
`broker.xml`.

The default value for this parameter is `true`.

For more information on core bridges and how to configure them, please
see [Core Bridges](core-bridges.md).

## Duplicate Detection and Cluster Connections

Cluster connections internally use core bridges to move messages
reliable between nodes of the cluster. Consequently they can also be
configured to insert the duplicate id header for each message they move
using their internal bridges.

To configure a cluster connection to add the duplicate id header, simply
set the `use-duplicate-detection` to `true` when configuring a cluster
connection in `broker.xml`.

The default value for this parameter is `true`.

For more information on cluster connections and how to configure them,
please see [Clusters](clusters.md).
