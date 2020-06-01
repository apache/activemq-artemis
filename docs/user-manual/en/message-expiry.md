# Message Expiry

Messages can be set with an optional *time to live* when sending them.

Apache ActiveMQ Artemis will not deliver a message to a consumer after it's
time to live has been exceeded. If the message hasn't been delivered by the
time that time to live is reached the server can discard it.

Apache ActiveMQ Artemis's addresses can be assigned an expiry address so that,
when messages are expired, they are removed from the queue and sent to the
expiry address. Many different queues can be bound to an expiry address.  These
*expired* messages can later be consumed for further inspection.

## Core API

Using Apache ActiveMQ Artemis Core API, you can set an expiration time directly
on the message:

```java
// message will expire in 5000ms from now
message.setExpiration(System.currentTimeMillis() + 5000);
```

JMS MessageProducer allows to set a TimeToLive for the messages it sent:

```java
// messages sent by this producer will be retained for 5s (5000ms) before expiration
producer.setTimeToLive(5000);
```

Expired messages get [special properties](copied-message-properties.md) plus this
additional property:

- `_AMQ_ACTUAL_EXPIRY`

  a Long property containing the *actual expiration time* of the
  expired message
  
## Configuring Expiry Delay

Default expiry delay can be configured in the address-setting configuration:

```xml
<!-- expired messages in exampleQueue will be sent to the expiry address expiryQueue -->
<address-setting match="exampleQueue">
   <expiry-address>expiryQueue</expiry-address>
   <expiry-delay>10</expiry-delay>
</address-setting>
```

`expiry-delay` defines the expiration time in milliseconds that will be used for messages 
which are using the default expiration time (i.e. 0). 
  
For example, if `expiry-delay` is set to "10" and a message which is using the default 
expiration time (i.e. 10) arrives then its expiration time of "0" will be changed to "10."
However, if a message which is using an expiration time of "20" arrives then its expiration
time will remain unchanged. Setting `expiry-delay` to "-1" will disable this feature. 
  
The default is `-1`.

If `expiry-delay` is *not set* then minimum and maximum expiry delay values can be configured
in the address-setting configuration.

```xml
<address-setting match="exampleQueue">
   <min-expiry-delay>10</min-expiry-delay>
   <max-expiry-delay>100</max-expiry-delay>
</address-setting>
```

Semantics are as follows:

 - Messages _without_ an expiration will be set to `max-expiry-delay`. If `max-expiry-delay`
   is not defined then the message will be set to `min-expiry-delay`. If `min-expiry-delay`
   is not defined then the message will not be changed.
 - Messages with an expiration _above_ `max-expiry-delay` will be set to `max-expiry-delay`
 - Messages with an expiration _below_ `min-expiry-delay` will be set to `min-expiry-delay`
 - Messages with an expiration _within_ `min-expiry-delay` and `max-expiry-delay` range will
   not be changed
 - Any value set for `expiry-delay` other than the default (i.e. `-1`) will override the
   aforementioned min/max settings.

The default for both `min-expiry-delay` and `max-expiry-delay` is `-1` (i.e. disabled).

## Configuring Expiry Addresses

Expiry address are defined in the address-setting configuration:

```xml
<!-- expired messages in exampleQueue will be sent to the expiry address expiryQueue -->
<address-setting match="exampleQueue">
   <expiry-address>expiryQueue</expiry-address>
</address-setting>
```

If messages are expired and no expiry address is specified, messages are simply
removed from the queue and dropped. Address [wildcards](wildcard-syntax.md) can
be used to configure expiry address for a set of addresses.

## Configuring Automatic Creation of Expiry Resources

It's common to segregate expired messages by their original address.
For example, a message sent to the `stocks` address that expired for some
reason might be ultimately routed to the `EXP.stocks` queue, and likewise
a message sent to the `orders` address that expired might be routed to
the `EXP.orders` queue.

Using this pattern can make it easy to track and administrate
expired messages. However, it can pose a challenge in environments
which predominantly use auto-created addresses and queues. Typically
administrators in those environments don't want to manually create
an `address-setting` to configure the `expiry-address` much less
the actual `address` and `queue` to hold the expired messages.

The solution to this problem is to set the `auto-create-expiry-resources`
`address-setting` to `true` (it's `false` by default) so that the broker will
create the `address` and `queue` to deal with the expired messages
automatically. The `address` created will be the one defined by the
`expiry-address`. A `MULTICAST` `queue` will be created on that `address`.
It will be named by the `address` to which the message was previously sent, and
it will have a filter defined using the property `_AMQ_ORIG_ADDRESS` so that it
will only receive messages sent to the relevant `address`. The `queue` name can
be configured with a prefix and suffix. See the relevant settings in the table
below:

`address-setting`|default
---|---
`expiry-queue-prefix`|`EXP.`
`expiry-queue-suffix`|(empty string)

Here is an example configuration:

```xml
<address-setting match="#">
   <expiry-address>expiryAddress</expiry-address>
   <auto-create-expiry-resources>true</auto-create-expiry-resources>
   <expiry-queue-prefix></expiry-queue-prefix> <!-- override the default -->
   <expiry-queue-suffix>.EXP</expiry-queue-suffix>
</address-setting>
```

The queue holding the expired messages can be accessed directly
either by using the queue's name by itself (e.g. when using the core
client) or by using the fully qualified queue name (e.g. when using
a JMS client) just like any other queue. Also, note that the queue is
auto-created which means it will be auto-deleted as per the relevant
`address-settings`.

## Configuring The Expiry Reaper Thread

A reaper thread will periodically inspect the queues to check if messages have
expired.

The reaper thread can be configured with the following properties in
`broker.xml`

- `message-expiry-scan-period`

  How often the queues will be scanned to detect expired messages (in
  milliseconds, default is 30000ms, set to `-1` to disable the reaper thread)

## Example

See the [Message Expiration Example](examples.md#message-expiration) which
shows how message expiry is configured and used with JMS.
