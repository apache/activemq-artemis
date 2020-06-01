# Message Redelivery and Undelivered Messages

Messages can be delivered unsuccessfully (e.g. if the transacted session
used to consume them is rolled back). Such a message goes back to its
queue ready to be redelivered. However, this means it is possible for a
message to be delivered again and again without success thus remaining
in the queue indefinitely, clogging the system.

There are 2 ways to deal with these undelivered messages:

- Delayed redelivery.

  It is possible to delay messages redelivery.  This gives the client some
  time to recover from any transient failures and to prevent overloading
  its network or CPU resources.

- Dead Letter Address.

  It is also possible to configure a dead letter address so that after
  a specified number of unsuccessful deliveries, messages are removed
  from their queue and sent to the dead letter address.  These messages
  will not be delivered again from this queue.

Both options can be combined for maximum flexibility.

## Delayed Redelivery

Delaying redelivery can often be useful in cases where clients regularly
fail or rollback. Without a delayed redelivery, the system can get into a
"thrashing" state, with delivery being attempted, the client rolling back,
and delivery being re-attempted ad infinitum in quick succession,
consuming valuable CPU and network resources.

### Configuring Delayed Redelivery

Delayed redelivery is defined in the address-setting configuration:

```xml
<!-- delay redelivery of messages for 5s -->
<address-setting match="exampleQueue">
   <!-- default is 1.0 -->
   <redelivery-delay-multiplier>1.5</redelivery-delay-multiplier>
   <!-- default is 0 (no delay) -->
   <redelivery-delay>5000</redelivery-delay>
   <!-- default is 0.0) -->
   <redelivery-collision-avoidance-factor>0.15</redelivery-collision-avoidance-factor>
   <!-- default is redelivery-delay * 10 -->
   <max-redelivery-delay>50000</max-redelivery-delay>
</address-setting>
```

If a `redelivery-delay` is specified, Apache ActiveMQ Artemis will wait this delay
before redelivering the messages.

By default, there is no redelivery delay (`redelivery-delay`is set to
0).

Other subsequent messages will be delivery regularly, only the cancelled
message will be sent asynchronously back to the queue after the delay.

You can specify a multiplier (the `redelivery-delay-multiplier`) that will
take effect on top of the `redelivery-delay`.  Each time a message is redelivered
the delay period will be equal to the previous delay * `redelivery-delay-multiplier`.
A `max-redelivery-delay` can be set to prevent the delay from becoming too large.
The `max-redelivery-delay` is defaulted to `redelivery-delay` \* 10.

**Example:**

- redelivery-delay=5000, redelivery-delay-multiplier=2, max-redelivery-delay=15000,
  redelivery-collision-avoidance-factor=0.0

1. Delivery Attempt 1. (Unsuccessful)
2. Wait Delay Period: 5000
3. Delivery Attempt 2. (Unsuccessful)
4. Wait Delay Period: 10000                   // (5000  * 2) < max-delay-period.  Use 10000
5. Delivery Attempt 3: (Unsuccessful)
6. Wait Delay Period: 15000                   // (10000 * 2) > max-delay-period:  Use max-delay-delivery

Address wildcards can be used to configure redelivery delay for a set of
addresses (see [Understanding the Wildcard Syntax](wildcard-syntax.md)), so you don't have to specify redelivery delay
individually for each address.

The `redelivery-delay` can be also be modified by configuring the
`redelivery-collision-avoidance-factor`. This factor will be made either
positive or negative at random to control whether the ultimate value will
increase or decrease the `redelivery-delay`. Then it's multiplied by a random
number between 0.0 and 1.0. This result is then multiplied by the
`redelivery-delay` and then added to the `redelivery-delay` to arrive at the
final value.

The algorithm may sound complicated but the bottom line is quite simple: the
larger `redelivery-collision-avoidance-factor` you choose the larger the variance
of the `redelivery-delay` will be. The `redelivery-collision-avoidance-factor`
must be between 0.0 and 1.0.

**Example:**

- redelivery-delay=1000, redelivery-delay-multiplier=1, max-redelivery-delay=15000,
  redelivery-collision-avoidance-factor=0.5, (bold values chosen using
  `java.util.Random`)

1. Delivery Attempt 1. (Unsuccessful)
2. Wait Delay Period: 875                     // 1000 + (1000 * ((0.5 \* __-1__) * __.25__)
3. Delivery Attempt 2. (Unsuccessful)
4. Wait Delay Period: 1375                    // 1000 + (1000 * ((0.5 \* __1__) * __.75__)
5. Delivery Attempt 3: (Unsuccessful)
6. Wait Delay Period: 975                     // 1000 + (1000 * ((0.5 \* __-1__) * __.05__)

This feature can be particularly useful in environments where there are
multiple consumers on the same queue all interacting transactionally
with the same external system (e.g. a database). If there is overlapping
data in messages which are consumed concurrently then one transaction can
succeed while all the rest fail. If those failed messages are redelivered
at the same time then this process where one consumer succeeds and the
rest fail will continue. By randomly padding the redelivery-delay by a
small, configurable amount these redelivery "collisions" can be avoided.

### Example

See [the examples chapter](examples.md) for an example which shows how
delayed redelivery is configured and used with JMS.

## Dead Letter Addresses

To prevent a client infinitely receiving the same undelivered message
(regardless of what is causing the unsuccessful deliveries), messaging
systems define *dead letter addresses*: after a specified unsuccessful
delivery attempts, the message is removed from its queue and sent
to a dead letter address.

Any such messages can then be diverted to queue(s) where they can later
be perused by the system administrator for action to be taken.

Apache ActiveMQ Artemis's addresses can be assigned a dead letter address. Once the
messages have been unsuccessfully delivered for a given number of
attempts, they are removed from their queue and sent to the relevant
dead letter address. These *dead letter* messages can later be consumed
from the dead letter address for further inspection.

### Configuring Dead Letter Addresses

Dead letter address is defined in the address-setting configuration:

```xml
<!-- undelivered messages in exampleQueue will be sent to the dead letter address
deadLetterQueue after 3 unsuccessful delivery attempts -->
<address-setting match="exampleQueue">
   <dead-letter-address>deadLetterAddress</dead-letter-address>
   <max-delivery-attempts>3</max-delivery-attempts>
</address-setting>
```

If a `dead-letter-address` is not specified, messages will removed after
`max-delivery-attempts` unsuccessful attempts.

By default, messages are redelivered 10 times at the maximum. Set
`max-delivery-attempts` to -1 for infinite redeliveries.

A `dead letter address` can be set globally for a set of matching
addresses and you can set `max-delivery-attempts` to -1 for a specific
address setting to allow infinite redeliveries only for this address.

Address wildcards can be used to configure dead letter settings for a
set of addresses (see [Understanding the Wildcard Syntax](wildcard-syntax.md)).

### Dead Letter Properties

Dead letter messages get [special properties](copied-message-properties.md).

### Automatically Creating Dead Letter Resources

It's common to segregate undelivered messages by their original address.
For example, a message sent to the `stocks` address that couldn't be
delivered for some reason might be ultimately routed to the `DLQ.stocks`
queue, and likewise a message sent to the `orders` address that couldn't
be delivered might be routed to the `DLQ.orders` queue.

Using this pattern can make it easy to track and administrate
undelivered messages. However, it can pose a challenge in environments
which predominantly use auto-created addresses and queues. Typically
administrators in those environments don't want to manually create
an `address-setting` to configure the `dead-letter-address` much less
the actual `address` and `queue` to hold the undelivered messages.

The solution to this problem is to set the `auto-create-dead-letter-resources`
`address-setting` to `true` (it's `false` by default) so that the broker will
create the `address` and `queue` to deal with the undelivered messages
automatically. The `address` created will be the one defined by the
`dead-letter-address`. A `MULTICAST` `queue` will be created on that `address`.
It will be named by the `address` to which the message was previously sent, and
it will have a filter defined using the property `_AMQ_ORIG_ADDRESS` so that it
 will only receive messages sent to the relevant `address`. The `queue` name
 can be configured with a prefix and suffix. See the relevant settings in the
 table below:

`address-setting`|default
---|---
`dead-letter-queue-prefix`|`DLQ.`
`dead-letter-queue-suffix`|(empty string)

Here is an example configuration:

```xml
<address-setting match="#">
   <dead-letter-address>DLA</dead-letter-address>
   <max-delivery-attempts>3</max-delivery-attempts>
   <auto-create-dead-letter-resources>true</auto-create-dead-letter-resources>
   <dead-letter-queue-prefix></dead-letter-queue-prefix> <!-- override the default -->
   <dead-letter-queue-suffix>.DLQ</dead-letter-queue-suffix>
</address-setting>
```

The queue holding the undeliverable messages can be accessed directly
either by using the queue's name by itself (e.g. when using the core
client) or by using the fully qualified queue name (e.g. when using
a JMS client) just like any other queue. Also, note that the queue is
auto-created which means it will be auto-deleted as per the relevant
`address-settings`.


### Example

See: Dead Letter section of the [Examples](examples.md) for an example
that shows how dead letter resources can be statically configured and
used with JMS.

## Delivery Count Persistence

In normal use, Apache ActiveMQ Artemis does not update delivery count *persistently*
until a message is rolled back (i.e. the delivery count is not updated
*before* the message is delivered to the consumer). In most messaging
use cases, the messages are consumed, acknowledged and forgotten as soon
as they are consumed. In these cases, updating the delivery count
persistently before delivering the message would add an extra persistent
step *for each message delivered*, implying a significant performance
penalty.

However, if the delivery count is not updated persistently before the
message delivery happens, in the event of a server crash, messages might
have been delivered but that will not have been reflected in the
delivery count. During the recovery phase, the server will not have
knowledge of that and will deliver the message with `redelivered` set to
`false` while it should be `true`.

As this behavior breaks strict JMS semantics, Apache ActiveMQ Artemis allows to persist
delivery count before message delivery but this feature is disabled by default
due to performance implications.

To enable it, set `persist-delivery-count-before-delivery` to `true` in
`broker.xml`:

```xml
<persist-delivery-count-before-delivery>true</persist-delivery-count-before-delivery>
```
