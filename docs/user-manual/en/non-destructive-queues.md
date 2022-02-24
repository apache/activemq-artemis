# Non-Destructive Queues

When a consumer attaches to a queue, the normal behaviour is that messages are
sent to that consumer are acquired exclusively by that consumer, and when the
consumer acknowledges them, the messages are removed from the queue.

Another common pattern is to have queue "browsers" which send all messages to
the browser, but do not prevent other consumers from receiving the messages,
and do not remove them from the queue when the browser is done with them. Such
a browser is an instance of a "non-destructive" consumer.

If every consumer on a queue is non destructive then we can obtain some
interesting behaviours. In the case of a [last value
queue](last-value-queues.md) then the queue will always contain the most up to
date value for every key.

A queue can be created to enforce all consumers are non-destructive using the
following queue configuration:

```xml
<address name="foo.bar">
   <multicast>
      <queue name="orders1" non-destructive="true" />
   </multicast>
</address>
```

Or on auto-create when using the JMS client by using address parameters when 
creating the destination used by the consumer.

```java
Queue queue = session.createQueue("my.destination.name?non-destructive=true");
Topic topic = session.createTopic("my.destination.name?non-destructive=true");
```

Also the default for all queues under and address can be defaulted using the 
`address-setting` configuration:

```xml
<address-setting match="nonDestructiveQueue">
   <default-non-destructive>true</default-non-destructive>
</address-setting>
```

By default, `default-non-destructive` is `false`.

## Limiting the Size of the Queue

For queues other than last-value queues, having only non-destructive consumers
could mean that messages would never get deleted, leaving the queue to grow 
without constraint. To prevent this you can use the ability to set a default
`expiry-delay`. See [expiry-delay](message-expiry.md#configuring-expiry-delay)
for more details on this. You could also use a [ring queue](ring-queues.md).