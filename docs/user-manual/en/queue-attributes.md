Queue Attributes
================

Queue attributes can be set in one of two ways. Either by configuring
them using the configuration file or by using the core API. This chapter
will explain how to configure each attribute and what effect the
attribute has.

Predefined Queues
=================

Queues can be predefined via configuration at a core level or at a JMS
level. Firstly let's look at a JMS level.

The following shows a queue predefined in the `activemq-jms.xml`
configuration file.

    <queue name="selectorQueue">
       <entry name="/queue/selectorQueue"/>
       <selector string="color='red'"/>
       <durable>true</durable>
    </queue>

This name attribute of queue defines the name of the queue. When we do
this at a jms level we follow a naming convention so the actual name of
the core queue will be `jms.queue.selectorQueue`.

The entry element configures the name that will be used to bind the
queue to JNDI. This is a mandatory element and the queue can contain
multiple of these to bind the same queue to different names.

The selector element defines what JMS message selector the predefined
queue will have. Only messages that match the selector will be added to
the queue. This is an optional element with a default of null when
omitted.

The durable element specifies whether the queue will be persisted. This
again is optional and defaults to true if omitted.

Secondly a queue can be predefined at a core level in the
`activemq-configuration.xml` file. The following is an example.

    <queues>
       <queue name="jms.queue.selectorQueue">
          <address>jms.queue.selectorQueue</address>
          <filter string="color='red'"/>
          <durable>true</durable>
        </queue>
    </queues>

This is very similar to the JMS configuration, with 3 real differences
which are.

1.  The name attribute of queue is the actual name used for the queue
    with no naming convention as in JMS.

2.  The address element defines what address is used for routing
    messages.

3.  There is no entry element.

4.  The filter uses the *Core filter syntax* (described in ?), *not* the
    JMS selector syntax.

Using the API
=============

Queues can also be created using the core API or the management API.

For the core API, queues can be created via the
`org.apache.activemq.api.core.client.ClientSession` interface. There are
multiple `createQueue` methods that support setting all of the
previously mentioned attributes. There is one extra attribute that can
be set via this API which is `temporary`. setting this to true means
that the queue will be deleted once the session is disconnected.

Take a look at ? for a description of the management API for creating
queues.

Configuring Queues Via Address Settings
=======================================

There are some attributes that are defined against an address wildcard
rather than a specific queue. Here an example of an `address-setting`
entry that would be found in the `activemq-configuration.xml` file.

    <address-settings>
       <address-setting match="jms.queue.exampleQueue">
          <dead-letter-address>jms.queue.deadLetterQueue</dead-letter-address>
          <max-delivery-attempts>3</max-delivery-attempts>
          <redelivery-delay>5000</redelivery-delay>
          <expiry-address>jms.queue.expiryQueue</expiry-address>
          <last-value-queue>true</last-value-queue>
          <max-size-bytes>100000</max-size-bytes>
          <page-size-bytes>20000</page-size-bytes>
          <redistribution-delay>0</redistribution-delay>
          <send-to-dla-on-no-route>true</send-to-dla-on-no-route>
          <address-full-policy>PAGE</address-full-policy>
          <slow-consumer-threshold>-1</slow-consumer-threshold>
          <slow-consumer-policy>NOTIFY</slow-consumer-policy>
          <slow-consumer-check-period>5</slow-consumer-check-period>
       </address-setting>
    </address-settings>

The idea with address settings, is you can provide a block of settings
which will be applied against any addresses that match the string in the
`match` attribute. In the above example the settings would only be
applied to any addresses which exactly match the address
`jms.queue.exampleQueue`, but you can also use wildcards to apply sets
of configuration against many addresses. The wildcard syntax used is
described [here](#wildcard-syntax).

For example, if you used the `match` string `jms.queue.#` the settings
would be applied to all addresses which start with `jms.queue.` which
would be all JMS queues.

The meaning of the specific settings are explained fully throughout the
user manual, however here is a brief description with a link to the
appropriate chapter if available.

`max-delivery-attempts` defines how many time a cancelled message can be
redelivered before sending to the `dead-letter-address`. A full
explanation can be found [here](#undelivered-messages.configuring).

`redelivery-delay` defines how long to wait before attempting redelivery
of a cancelled message. see [here](#undelivered-messages.delay).

`expiry-address` defines where to send a message that has expired. see
[here](#message-expiry.configuring).

`expiry-delay` defines the expiration time that will be used for
messages which are using the default expiration time (i.e. 0). For
example, if `expiry-delay` is set to "10" and a message which is using
the default expiration time (i.e. 0) arrives then its expiration time of
"0" will be changed to "10." However, if a message which is using an
expiration time of "20" arrives then its expiration time will remain
unchanged. Setting `expiry-delay` to "-1" will disable this feature. The
default is "-1".

`last-value-queue` defines whether a queue only uses last values or not.
see [here](#last-value-queues).

`max-size-bytes` and `page-size-bytes` are used to set paging on an
address. This is explained [here](#paging).

`redistribution-delay` defines how long to wait when the last consumer
is closed on a queue before redistributing any messages. see
[here](#clusters).

`send-to-dla-on-no-route`. If a message is sent to an address, but the
server does not route it to any queues, for example, there might be no
queues bound to that address, or none of the queues have filters that
match, then normally that message would be discarded. However if this
parameter is set to true for that address, if the message is not routed
to any queues it will instead be sent to the dead letter address (DLA)
for that address, if it exists.

`address-full-policy`. This attribute can have one of the following
values: PAGE, DROP, FAIL or BLOCK and determines what happens when an
address where `max-size-bytes` is specified becomes full. The default
value is PAGE. If the value is PAGE then further messages will be paged
to disk. If the value is DROP then further messages will be silently
dropped. If the value is FAIL then further messages will be dropped and
an exception will be thrown on the client-side. If the value is BLOCK
then client message producers will block when they try and send further
messages. See the following chapters for more info ?, ?.

`slow-consumer-threshold`. The minimum rate of message consumption
allowed before a consumer is considered "slow." Measured in
messages-per-second. Default is -1 (i.e. disabled); any other valid
value must be greater than 0.

`slow-consumer-policy`. What should happen when a slow consumer is
detected. `KILL` will kill the consumer's connection (which will
obviously impact any other client threads using that same connection).
`NOTIFY` will send a CONSUMER\_SLOW management notification which an
application could receive and take action with. See ? for more details
on this notification.

`slow-consumer-check-period`. How often to check for slow consumers on a
particular queue. Measured in minutes. Default is 5. See ? for more
information about slow consumer detection.
