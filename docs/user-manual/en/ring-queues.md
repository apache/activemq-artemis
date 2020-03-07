# Ring Queue

Queues operate with first-in, first-out (FIFO) semantics which means that
messages, in general, are added to the "tail" of the queue and removed from the
"head." A "ring" queue is a special type of queue with a *fixed* size. The
fixed size is maintained by removing the message at the head of the queue when
the number of messages on the queue reaches the configured size.

For example, consider a queue configured with a ring size of 3 and a producer
which sends the messages `A`, `B`, `C`, & `D` in that order. Once `C` is sent
the number of messages in the queue will be 3 which is the same as the
configured ring size. We can visualize the queue growth like this...

After `A` is sent:
```
             |---|
head/tail -> | A |
             |---|
```

After `B` is sent:

```
        |---|
head -> | A |
        |---|
tail -> | B |
        |---|
```

After `C` is sent:

```
        |---|
head -> | A |
        |---|
        | B |
        |---|
tail -> | C |
        |---|
```

When `D` is sent it will be added to the tail of the queue and the message at
the head of the queue (i.e. `A`) will be removed so the queue will look like
this:

```
        |---|
head -> | B |
        |---|
        | C |
        |---|
tail -> | D |
        |---|
```

This example covers the most basic use case with messages being added to the
tail of the queue. However, there are a few other important use cases
involving:

 - Messages in delivery & rollbacks
 - Scheduled messages
 - Paging

However, before we get to those use cases let's look at the basic configuration
of a ring queue.

## Configuration

There are 2 parameters related to ring queue configuration.

The `ring-size` parameter can be set directly on the `queue` element. The
default value comes from the `default-ring-size` `address-setting` (see below).

```xml
<addresses>
   <address name="myRing">
      <anycast>
         <queue name="myRing" ring-size="3" />
      </anycast>
   </address>
</addresses>
```

The `default-ring-size` is an `address-setting` which applies to queues on
matching addresses which don't have an explicit `ring-size` set. This is
especially useful for auto-created queues. The default value is `-1` (i.e.
no limit).

```xml
<address-settings>
   <address-setting match="ring.#">
      <default-ring-size>3</default-ring-size>
   </address-setting>
</address-settings>
```

The `ring-size` may be updated at runtime. If the new `ring-size` is set
*lower* than the previous `ring-size` the broker will not immediately delete
enough messages from the head of the queue to enforce the new size. New
messages sent to the queue will force the deletion of old messages (i.e. the
queue won't grow any larger), but the queue will not reach its new size until
it does so *naturally* through the normal consumption of messages by
clients.

## Messages in Delivery & Rollbacks

When messages are "in delivery" they are in an in-between state where they are
not technically on the queue but they are also not yet acknowledged. The
broker is at the consumer’s mercy to either acknowledge such messages or not.
In the context of a ring queue, messages which are in-delivery cannot be
removed from the queue.

This presents a few dilemmas.

Due to the nature of messages in delivery a client can actually send more
messages to a ring queue than it would otherwise permit. This can make it
appear that the ring-size is not being enforced properly. Consider this
simple scenario:

 - Queue `foo` with `ring-size="3"`
 - 1 Consumer on queue `foo`
 - Message `A` sent to `foo` & dispatched to consumer
 - `messageCount`=1, `deliveringCount`=1
 - Message `B` sent to `foo` & dispatched to consumer
 - `messageCount`=2, `deliveringCount`=2
 - Message `C` sent to `foo` & dispatched to consumer
 - `messageCount`=3, `deliveringCount`=3
 - Message `D` sent to `foo` & dispatched to consumer
 - `messageCount`=4, `deliveringCount`=4

The `messageCount` for `foo` is now 4, one *greater* than the `ring-size`
of 3! However, the broker has no choice but to allow this because it cannot
remove messages from the queue which are in delivery.

Now consider that the consumer is closed without actually acknowledging any
of these 4 messages. These 4 in-delivery, unacknowledged messages will be
cancelled back to the broker and added to the *head* of the queue in the
reverse order from which they were consumed. This, of course, will put the
queue over its configured `ring-size`. Therefore, since a ring queue
prefers messages at the tail of the queue over messages at the head it will
keep `B`, `C`, & `D` and delete `A` (since `A` was the last message added
to the head of the queue).

Transaction or core session rollbacks are treated the same way.

If you wish to avoid these kinds of situations and you're using the core
client directly or the core JMS client you can minimize messages in delivery
by reducing the size of `consumerWindowSize` (1024 * 1024 bytes by default).

## Scheduled Messages

When a scheduled message is sent to a queue it isn't immediately added to the
tail of the queue like normal messages. It is held in an intermediate buffer
and scheduled for delivery onto the *head* of the queue according to the
details of the message. However, scheduled messages are nevertheless reflected
in the message count of the queue. As with messages which are in delivery this
can make it appear that the ring queue's size is not being enforced. Consider
this simple scenario:

 - Queue `foo` with `ring-size="3"`
 - At 12:00 message `A` sent to `foo` scheduled for 12:05
 - `messageCount`=1, `scheduledCount`=1
 - At 12:01 message `B` sent to `foo`
 - `messageCount`=2, `scheduledCount`=1
 - At 12:02 message `C` sent to `foo`
 - `messageCount`=3, `scheduledCount`=1
 - At 12:03 message `D` sent to `foo`
 - `messageCount`=4, `scheduledCount`=1

The `messageCount` for `foo` is now 4, one *greater* than the `ring-size` of 3!
However, the scheduled message is not technically on the queue yet (i.e. it is
on the broker and scheduled to be put on the queue). When the scheduled
delivery time for 12:05 comes the message will put on the head of the queue,
but since the ring queue's size has already been reach the scheduled message
`A` will be removed.

## Paging

Similar to scheduled messages and messages in delivery, paged messages don't
count against a ring queue's size because messages are actually paged at the
*address* level, not the queue level. A paged message is not technically on a
queue although it is reflected in a queue's `messageCount`.

It is recommended that paging is not used for addresses with ring queues. In
other words, ensure that the entire address will be able to fit into memory or
use the `DROP`, `BLOCK` or `FAIL` `address-full-policy`.