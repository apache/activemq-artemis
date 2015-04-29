# Diverting and Splitting Message Flows

Apache ActiveMQ Artemis allows you to configure objects called *diverts* with some
simple server configuration.

Diverts allow you to transparently divert messages routed to one address
to some other address, without making any changes to any client
application logic.

Diverts can be *exclusive*, meaning that the message is diverted to the
new address, and does not go to the old address at all, or they can be
*non-exclusive* which means the message continues to go the old address,
and a *copy* of it is also sent to the new address. Non-exclusive
diverts can therefore be used for *splitting* message flows, e.g. there
may be a requirement to monitor every order sent to an order queue.

Diverts can also be configured to have an optional message filter. If
specified then only messages that match the filter will be diverted.

Diverts can also be configured to apply a `Transformer`. If specified,
all diverted messages will have the opportunity of being transformed by
the `Transformer`.

A divert will only divert a message to an address on the *same server*,
however, if you want to divert to an address on a different server, a
common pattern would be to divert to a local store-and-forward queue,
then set up a bridge which consumes from that queue and forwards to an
address on a different server.

Diverts are therefore a very sophisticated concept, which when combined
with bridges can be used to create interesting and complex routings. The
set of diverts on a server can be thought of as a type of routing table
for messages. Combining diverts with bridges allows you to create a
distributed network of reliable routing connections between multiple
geographically distributed servers, creating your global messaging mesh.

Diverts are defined as xml in the `broker.xml` file.
There can be zero or more diverts in the file.

Please see the examples for a full working example showing you how to
configure and use diverts.

Let's take a look at some divert examples:

## Exclusive Divert

Let's take a look at an exclusive divert. An exclusive divert diverts
all matching messages that are routed to the old address to the new
address. Matching messages do not get routed to the old address.

Here's some example xml configuration for an exclusive divert, it's
taken from the divert example:

    <divert name="prices-divert">
       <address>jms.topic.priceUpdates</address>
       <forwarding-address>jms.queue.priceForwarding</forwarding-address>
       <filter string="office='New York'"/>
       <transformer-class-name>
          org.apache.activemq.artemis.jms.example.AddForwardingTimeTransformer
       </transformer-class-name>
       <exclusive>true</exclusive>
    </divert>

We define a divert called '`prices-divert`' that will divert any
messages sent to the address '`jms.topic.priceUpdates`' (this
corresponds to any messages sent to a JMS Topic called '`priceUpdates`')
to another local address '`jms.queue.priceForwarding`' (this corresponds
to a local JMS queue called '`priceForwarding`'

We also specify a message filter string so only messages with the
message property `office` with value `New York` will get diverted, all
other messages will continue to be routed to the normal address. The
filter string is optional, if not specified then all messages will be
considered matched.

In this example a transformer class is specified. Again this is
optional, and if specified the transformer will be executed for each
matching message. This allows you to change the messages body or
properties before it is diverted. In this example the transformer simply
adds a header that records the time the divert happened.

This example is actually diverting messages to a local store and forward
queue, which is configured with a bridge which forwards the message to
an address on another ActiveMQ server. Please see the example for more
details.

## Non-exclusive Divert

Now we'll take a look at a non-exclusive divert. Non exclusive diverts
are the same as exclusive diverts, but they only forward a *copy* of the
message to the new address. The original message continues to the old
address

You can therefore think of non-exclusive diverts as *splitting* a
message flow.

Non exclusive diverts can be configured in the same way as exclusive
diverts with an optional filter and transformer, here's an example
non-exclusive divert, again from the divert example:

    <divert name="order-divert">
        <address>jms.queue.orders</address>
        <forwarding-address>jms.topic.spyTopic</forwarding-address>
        <exclusive>false</exclusive>
    </divert>

The above divert example takes a copy of every message sent to the
address '`jms.queue.orders`' (Which corresponds to a JMS Queue called
'`orders`') and sends it to a local address called
'`jms.topic.SpyTopic`' (which corresponds to a JMS Topic called
'`SpyTopic`').
