# Message Expiry

Messages can be set with an optional *time to live* when sending them.

Apache ActiveMQ Artemis will not deliver a message to a consumer after it's time to
live has been exceeded. If the message hasn't been delivered by the time
that time to live is reached the server can discard it.

Apache ActiveMQ Artemis's addresses can be assigned a expiry address so that, when
messages are expired, they are removed from the queue and sent to the
expiry address. Many different queues can be bound to an expiry address.
These *expired* messages can later be consumed for further inspection.

## Core API

Using Apache ActiveMQ Artemis Core API, you can set an expiration time directly on the
message:

```java
// message will expire in 5000ms from now
message.setExpiration(System.currentTimeMillis() + 5000);
```

JMS MessageProducer allows to set a TimeToLive for the messages it sent:

```java
// messages sent by this producer will be retained for 5s (5000ms) before expiration
producer.setTimeToLive(5000);
```

Expired messages which are consumed from an expiry address have the
following properties:

-   `_AMQ_ORIG_ADDRESS`

    a String property containing the *original address* of the expired
    message

-   `_AMQ_ORIG_QUEUE`

    a String property containing the *original queue* of the expired
    message

-   `_AMQ_ACTUAL_EXPIRY`

    a Long property containing the *actual expiration time* of the
    expired message

## Configuring Expiry Addresses

Expiry address are defined in the address-setting configuration:

```xml
<!-- expired messages in exampleQueue will be sent to the expiry address expiryQueue -->
<address-setting match="exampleQueue">
   <expiry-address>expiryQueue</expiry-address>
</address-setting>
```

If messages are expired and no expiry address is specified, messages are
simply removed from the queue and dropped. Address wildcards can be used
to configure expiry address for a set of addresses (see [Understanding the Wildcard Syntax](wildcard-syntax.md)).

## Configuring The Expiry Reaper Thread

A reaper thread will periodically inspect the queues to check if
messages have expired.

The reaper thread can be configured with the following properties in
`broker.xml`

-   `message-expiry-scan-period`

    How often the queues will be scanned to detect expired messages (in
    milliseconds, default is 30000ms, set to `-1` to disable the reaper
    thread)

-   `message-expiry-thread-priority`

    The reaper thread priority (it must be between 1 and 10, 10 being the
    highest priority, default is 3)

## Example

See the [examples.md](examples.md) chapter for an example which shows how message expiry is configured and used with JMS.
