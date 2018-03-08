# Message Redelivery and Undelivered Messages

Messages can be delivered unsuccessfully (e.g. if the transacted session
used to consume them is rolled back). Such a message goes back to its
queue ready to be redelivered. However, this means it is possible for a
message to be delivered again and again without success thus remaining
in the queue indefinitely, clogging the system.

There are 2 ways to deal with these undelivered messages:

-   Delayed redelivery.

    It is possible to delay messages redelivery.  This gives the client some
    time to recover from any transient failures and to prevent overloading
    its network or CPU resources.

-   Dead Letter Address.

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
A max-redelivery-delay can be set to prevent the delay from becoming too large.
The max-redelivery-delay is defaulted to redelivery-delay \* 10.

Example:

    - redelivery-delay=5000, redelivery-delay-multiplier=2, max-redelivery-delay=15000

    1. Delivery Attempt 1. (Unsuccessful)
    2. Wait Delay Period: 5000
    3. Delivery Attempt 2. (Unsuccessful)
    4. Wait Delay Period: 10000                   // (5000  * 2) < max-delay-period.  Use 10000
    5. Delivery Attempt 3: (Unsuccessful)
    6. Wait Delay Period: 15000                   // (10000 * 2) > max-delay-period:  Use max-delay-delivery

Address wildcards can be used to configure redelivery delay for a set of
addresses (see [Understanding the Wildcard Syntax](wildcard-syntax.md)), so you don't have to specify redelivery delay
individually for each address.

### Example

See [the examples chapter](examples.md) for an example which shows how delayed redelivery is configured
and used with JMS.

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
   <dead-letter-address>deadLetterQueue</dead-letter-address>
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

Dead letter messages which are consumed from a dead letter address have
the following properties:

-   `_AMQ_ORIG_ADDRESS`

    a String property containing the *original address* of the dead
    letter message

-   `_AMQ_ORIG_QUEUE`

    a String property containing the *original queue* of the dead letter
    message

### Example

See: Dead Letter section of the [Examples](examples.md) for an example
that shows how dead letter is configured and used with JMS.

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
