# Configuring Addresses and Queues via Address Settings

There are some attributes that are defined against an address wildcard rather
than a specific address/queue. Here an example of an `address-setting` entry
that would be found in the `broker.xml` file.

```xml
<address-settings>
   <address-setting match="order.foo">
      <dead-letter-address>DLA</dead-letter-address>
      <auto-create-dead-letter-resources>false</auto-create-dead-letter-resources>
      <dead-letter-queue-prefix>DLQ.</dead-letter-queue-prefix>
      <dead-letter-queue-suffix></dead-letter-queue-suffix>
      <expiry-address>ExpiryQueue</expiry-address>
      <auto-create-expiry-resources>false</auto-create-expiry-resources>
      <expiry-queue-prefix>EXP.</expiry-queue-prefix>
      <expiry-queue-suffix></expiry-queue-suffix>
      <expiry-delay>123</expiry-delay>
      <redelivery-delay>5000</redelivery-delay>
      <redelivery-delay-multiplier>1.0</redelivery-delay-multiplier>
      <redelivery-collision-avoidance-factor>0.0</redelivery-collision-avoidance-factor>
      <max-redelivery-delay>10000</max-redelivery-delay>
      <max-delivery-attempts>3</max-delivery-attempts>
      <max-size-bytes>100000</max-size-bytes>
      <max-size-messages>1000</max-size-messages>
      <max-size-bytes-reject-threshold>-1</max-size-bytes-reject-threshold>
      <page-size-bytes>20000</page-size-bytes>
      <address-full-policy>PAGE</address-full-policy>
      <message-counter-history-day-limit></message-counter-history-day-limit>
      <last-value-queue>true</last-value-queue> <!-- deprecated! see default-last-value-queue -->
      <default-last-value-queue>false</default-last-value-queue>
      <default-non-destructive>false</default-non-destructive>
      <default-exclusive-queue>false</default-exclusive-queue>
      <default-consumers-before-dispatch>0</default-consumers-before-dispatch>
      <default-delay-before-dispatch>-1</default-delay-before-dispatch>
      <redistribution-delay>0</redistribution-delay>
      <send-to-dla-on-no-route>false</send-to-dla-on-no-route>
      <slow-consumer-threshold>-1</slow-consumer-threshold>
      <slow-consumer-threshold-measurement-unit>MESSAGES_PER_SECOND</slow-consumer-threshold-measurement-unit>
      <slow-consumer-policy>NOTIFY</slow-consumer-policy>
      <slow-consumer-check-period>5</slow-consumer-check-period>
      <auto-create-queues>true</auto-create-queues>
      <auto-delete-queues>true</auto-delete-queues>
      <auto-delete-created-queues>false</auto-delete-created-queues>
      <auto-delete-queues-delay>0</auto-delete-queues-delay>
      <auto-delete-queues-message-count>0</auto-delete-queues-message-count>
      <config-delete-queues>OFF</config-delete-queues>
      <config-delete-diverts>OFF</config-delete-diverts>
      <auto-create-addresses>true</auto-create-addresses>
      <auto-delete-addresses>true</auto-delete-addresses>
      <auto-delete-addresses-delay>0</auto-delete-addresses-delay>
      <config-delete-addresses>OFF</config-delete-addresses>
      <management-browse-page-size>200</management-browse-page-size>
      <management-message-attribute-size-limit>256</management-message-attribute-size-limit>
      <default-purge-on-no-consumers>false</default-purge-on-no-consumers>
      <default-max-consumers>-1</default-max-consumers>
      <default-queue-routing-type></default-queue-routing-type>
      <default-address-routing-type></default-address-routing-type>
      <default-ring-size>-1</default-ring-size>
      <retroactive-message-count>0</retroactive-message-count>
      <enable-metrics>true</enable-metrics>
      <enable-ingress-timestamp>false</enable-ingress-timestamp>
   </address-setting>
</address-settings>
```

The idea with address settings, is you can provide a block of settings which
will be applied against any addresses that match the string in the `match`
attribute. In the above example the settings would only be applied to the
address "order.foo" address but you can also use
[wildcards](wildcard-syntax.md) to apply settings.

For example, if you used the `match` string `queue.#` the settings would be
applied to all addresses which start with `queue.`

Address settings are **hierarchical**. Therefore, if more than one
`address-setting` would match then the settings are applied in order of their
specificity with the more specific match taking priority. A match on the
any-words delimiter (`#`) is considered less specific than a match without it.
A match with a single word delimiter `*` is considered less specific than a
match on an exact queue name. In this way settings can be "layered" so that
configuration details don't need to be repeated.

The meaning of the specific settings are explained fully throughout the user
manual, however here is a brief description with a link to the appropriate
chapter if available.

`dead-letter-address` is the address to which messages are sent when they
exceed `max-delivery-attempts`. If no address is defined here then such
messages will simply be discarded. Read more about [undelivered
messages](undelivered-messages.md#configuring-dead-letter-addresses).

`auto-create-dead-letter-resources` determines whether or not the broker will
automatically create the defined `dead-letter-address` and a corresponding
dead-letter queue when a message is undeliverable. Read more in the chapter
about [undelivered messages](undelivered-messages.md).

`dead-letter-queue-prefix` defines the prefix used for automatically created
dead-letter queues. Read more in the chapter about
[undelivered messages](undelivered-messages.md).

`dead-letter-queue-suffix` defines the suffix used for automatically created
dead-letter queues. Read more in the chapter about
[undelivered messages](undelivered-messages.md).

`expiry-address` defines where to send a message that has expired. If no
address is defined here then such messages will simply be discarded. Read more
about [message expiry](message-expiry.md#configuring-expiry-addresses).

`auto-create-expiry-resources` determines whether or not the broker will
automatically create the defined `expiry-address` and a corresponding expiry
queue when a message expired. Read more in the chapter about
[undelivered messages](undelivered-messages.md).

`expiry-queue-prefix` defines the prefix used for automatically created expiry
queues. Read more in the chapter about [message expiry](message-expiry.md).

`expiry-queue-suffix` defines the suffix used for automatically created expiry
queues. Read more in the chapter about [message expiry](message-expiry.md).

`expiry-delay` defines the expiration time that will be used for messages which
are using the default expiration time (i.e. 0). For example, if `expiry-delay`
is set to "10" and a message which is using the default expiration time (i.e.
0) arrives then its expiration time of "0" will be changed to "10." However, if
a message which is using an expiration time of "20" arrives then its expiration
time will remain unchanged. Setting `expiry-delay` to "-1" will disable this
feature. The default is "-1". Read more about [message
expiry](message-expiry.md#configuring-expiry-addresses).

`max-delivery-attempts` defines how many time a cancelled message can be
redelivered before sending to the `dead-letter-address`. Read more about
[undelivered
messages](undelivered-messages.md#configuring-dead-letter-addresses).

`redelivery-delay` defines how long to wait before attempting redelivery of a
cancelled message. Default is `0`. Read more about [undelivered
messages](undelivered-messages.md#configuring-delayed-redelivery).

`redelivery-delay-multiplier` defines the number by which the
`redelivery-delay` will be multiplied on each subsequent redelivery attempt.
Default is `1.0`. Read more about [undelivered
messages](undelivered-messages.md#configuring-delayed-redelivery).

`redelivery-collision-avoidance-factor` defines an additional factor used to
calculate an adjustment to the `redelivery-delay` (up or down). Default is
`0.0`. Valid values are between 0.0 and 1.0. Read more about [undelivered
messages](undelivered-messages.md#configuring-delayed-redelivery).

`max-size-bytes`, `max-size-messages`, `page-size-bytes`, `max-read-page-messages` & `max-read-page-bytes` are used to
configure paging on an address. This is explained
[here](paging.md#configuration).

`max-size-bytes-reject-threshold` is used with the address full `BLOCK` policy,
the maximum size (in bytes) an address can reach before messages start getting
rejected. Works in combination with `max-size-bytes` **for AMQP clients only**.
Default is `-1` (i.e. no limit).

`address-full-policy`. This attribute can have one of the following values:
`PAGE`, `DROP`, `FAIL` or `BLOCK` and determines what happens when an address
where `max-size-bytes` is specified becomes full. The default value is `PAGE`.
If the value is `PAGE` then further messages will be paged to disk. If the
value is `DROP` then further messages will be silently dropped. If the value is
`FAIL` then further messages will be dropped and an exception will be thrown on
the client-side. If the value is `BLOCK` then client message producers will
block when they try and send further messages.  See the [Flow
Control](flow-control.md) and [Paging](paging.md) chapters for more info.

`message-counter-history-day-limit` is the number of days to keep message
counter history for this address assuming that `message-counter-enabled` is
`true`. Default is `0`.

`default-last-value-queue` defines whether a queue only uses last values or
not. Default is `false`. This value can be overridden at the queue level using
the `last-value` boolean. Read more about [last value
queues](last-value-queues.md).

`default-exclusive-queue` defines whether a queue will serve only a single
consumer. Default is `false`. This value can be overridden at the queue level
using the `exclusive` boolean. Read more about [exclusive
queues](exclusive-queues.md).

`default-consumers-before-dispatch` defines the number of consumers needed on a
queue bound to the matching address before messages will be dispatched to those
consumers. Default is `0`. This value can be overridden at the queue level using
the `consumers-before-dispatch` boolean. This behavior can be tuned using
`delay-before-dispatch` on the queue itself or by using the
`default-delay-before-dispatch` address-setting.

`default-delay-before-dispatch` defines the number of milliseconds the broker
will wait for the configured number of consumers to connect to the matching queue
before it will begin to dispatch messages. Default is `-1` (wait forever).

`redistribution-delay` defines how long to wait when the last consumer is
closed on a queue before redistributing any messages. Read more about
[clusters](clusters.md#message-redistribution).

`send-to-dla-on-no-route`. If a message is sent to an address, but the server
does not route it to any queues (e.g. there might be no queues bound to that
address, or none of the queues have filters that match) then normally that
message would be discarded. However, if this parameter is `true` then such a
message will instead be sent to the `dead-letter-address` (DLA) for that
address, if it exists. Default is `false`.

`slow-consumer-threshold`. The minimum rate of message consumption allowed
before a consumer is considered "slow." Measured in units specified by the
slow-consumer-threshold-measurement-unit configuration option. Default is `-1`
 (i.e. disabled); any other value must be greater than 0 to ensure a queue
has messages, and it is the actual consumer that is slow. A value of 0 will
allow a consumer with no messages pending to be considered slow.
 Read more about [slow consumers](slow-consumers.md).

`slow-consumer-threshold-measurement-unit`. The units used to measure the 
slow-consumer-threshold.  Valid options are:
* MESSAGES_PER_SECOND
* MESSAGES_PER_MINUTE
* MESSAGES_PER_HOUR
* MESSAGES_PER_DAY 

If no unit is specified the default MESSAGES_PER_SECOND will be used.
Read more about [slow consumers](slow-consumers.md).

`slow-consumer-policy`. What should happen when a slow consumer is detected.
`KILL` will kill the consumer's connection (which will obviously impact any
other client threads using that same connection). `NOTIFY` will send a
CONSUMER\_SLOW management notification which an application could receive and
take action with. Read more about [slow consumers](slow-consumers.md).

`slow-consumer-check-period`. How often to check for slow consumers on a
particular queue. Measured in *seconds*. Default is `5`. 

* Note: This should be at least 2x the maximum time it takes a consumer to process
1 message.  For example, if the slow-consumer-threshold is set to 1 and the 
slow-consumer-threshold-measurement-unit is set to MESSAGES_PER_MINUTE then this
should be set to at least 2 x 60s i.e. 120s.
Read more about [slow
consumers](slow-consumers.md).

`auto-create-queues`. Whether or not the broker should automatically create a
queue when a message is sent or a consumer tries to connect to a queue whose
name fits the address `match`. Queues which are auto-created are durable,
non-temporary, and non-transient. Default is `true`. **Note:** automatic queue
creation does *not* work for the core client. The core API is a low-level API
and is not meant to have such automation.

`auto-delete-queues`. Whether or not the broker should automatically delete
auto-created queues when they have both 0 consumers and the message count is 
less than or equal to `auto-delete-queues-message-count`. Default is
`true`.

`auto-delete-created-queues`. Whether or not the broker should automatically delete
created queues when they have both 0 consumers and the message count is 
less than or equal to `auto-delete-queues-message-count`. Default is
`false`.

`auto-delete-queues-delay`. How long to wait (in milliseconds) before deleting
auto-created queues after the queue has 0 consumers and the message count is 
less than or equal to `auto-delete-queues-message-count`. 
Default is `0` (delete immediately). The broker's `address-queue-scan-period` controls
how often (in milliseconds) queues are scanned for potential deletion. Use `-1`
to disable scanning. The default scan value is `30000`.

`auto-delete-queues-message-count`. The message count that the queue must be 
less than or equal to before deleting auto-created queues. 
To disable message count check `-1` can be set.
Default is `0` (empty queue).

**Note:** the above auto-delete address settings can also be configured 
individually at the queue level when a client auto creates the queue.
 
For Core API it is exposed in createQueue methods. 

For Core JMS you can set it using the destination queue attributes
`my.destination?auto-delete=true&auto-delete-delay=120000&auto-delete-message-count=-1`

`config-delete-queues`. How the broker should handle queues deleted on config
reload, by delete policy: `OFF` or `FORCE`.  Default is `OFF`. Read more about
[configuration reload](config-reload.md).

`config-delete-diverts`. How the broker should handle diverts deleted on config
reload, by delete policy: `OFF` or `FORCE`.  Default is `OFF`. Read more about
[configuration reload](config-reload.md).
`auto-create-addresses`. Whether or not the broker should automatically create
an address when a message is sent to or a consumer tries to consume from a
queue which is mapped to an address whose name fits the address `match`.
Default is `true`. **Note:** automatic address creation does *not* work for the
core client. The core API is a low-level API and is not meant to have such
automation.

`auto-delete-addresses`. Whether or not the broker should automatically delete
auto-created addresses once the address no longer has any queues. Default is
`true`.

`auto-delete-addresses-delay`. How long to wait (in milliseconds) before
deleting auto-created addresses after they no longer have any queues. Default
is `0` (delete immediately). The broker's `address-queue-scan-period` controls
how often (in milliseconds) addresses are scanned for potential deletion. Use
`-1` to disable scanning. The default scan value is `30000`.

`config-delete-addresses`. How the broker should handle addresses deleted on
config reload, by delete policy: `OFF` or `FORCE`. Default is `OFF`. Read more
about [configuration reload](config-reload.md).

`management-browse-page-size` is the number of messages a management resource
can browse. This is relevant for the `browse, list and count-with-filter` management
methods exposed on the queue control. Default is `200`.

`management-message-attribute-size-limit` is the number of bytes collected from
the message for browse. This is relevant for the `browse and list` management
methods exposed on the queue control. Message attributes longer than this value
appear truncated. Default is `256`. Use `-1` to switch this limit off. Note that
memory needs to be allocated for all messages that are visible at a given moment.
Setting this value too high may impact the browser stability due to the large
amount of memory that may be required to browse through many messages.

`default-purge-on-no-consumers` defines a queue's default
`purge-on-no-consumers` setting if none is provided on the queue itself.
Default is `false`. This value can be overridden at the queue level using the
`purge-on-no-consumers` boolean. Read more about [this
functionality](#non-durable-subscription-queue).

`default-max-consumers` defines a queue's default `max-consumers` setting if
none is provided on the  queue itself.  Default is `-1` (i.e. no limit). This
value can be overridden at the queue level using the `max-consumers` boolean.
Read more about [this
functionality](#shared-durable-subscription-queue-using-max-consumers).

`default-queue-routing-type` defines the routing-type for an auto-created queue
if the broker is unable to determine the routing-type based on the client
and/or protocol semantics. Default is `MULTICAST`. Read more about [routing
types](#routing-type).

`default-address-routing-type` defines the routing-type for an auto-created
address if the broker is unable to determine the routing-type based on the
client and/or protocol semantics. Default is `MULTICAST`. Read more about
[routing types](#routing-type).

`default-consumer-window-size` defines the default `consumerWindowSize` value 
for a `CORE` protocol consumer, if not defined the default will be set to 
1 MiB (1024 * 1024 bytes). The consumer will use this value as the window size
if the value is not set on the client. Read more about
[flow control](flow-control.md).

`default-ring-size` defines the default `ring-size` value for any matching queue
which doesn't have `ring-size` explicitly defined. If not defined the default will
be set to -1. Read more about [ring queues](ring-queues.md).

`retroactive-message-count` defines the number of messages to preserve for future
queues created on the matching address. Defaults to 0. Read more about
[retroactive addresses](retroactive-addresses.md).

`enable-metrics` determines whether or not metrics will be published to any
configured metrics plugin for the matching address. Default is `true`. Read more
about [metrics](metrics.md).

`enable-ingress-timestamp` determines whether or not the broker will add its time 
to messages sent to the matching address. When `true` the exact behavior will 
depend on the specific protocol in use. For AMQP messages the broker will add a
`long` *message annotation* named `x-opt-ingress-time`. For core messages (used by
the core and OpenWire protocols) the broker will add a long property named
`_AMQ_INGRESS_TIMESTAMP`. For STOMP messages the broker will add a frame header 
named `ingress-timestamp`. The value will be the number of milliseconds since the
[epoch](https://en.wikipedia.org/wiki/Unix_time). Default is `false`.
