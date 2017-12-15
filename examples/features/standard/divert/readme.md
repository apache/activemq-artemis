# Divert Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

ActiveMQ Artemis diverts allow messages to be transparently "diverted" from one address to another with just some simple configuration defined on the broker side.

Diverts can be defined to be **exclusive** or **non-exclusive**.

With an **exclusive** divert the message is intercepted and does not get sent to the queues originally bound to that address - it only gets diverted.

With a **non-exclusive** divert the message continues to go to the queues bound to the address, but also a **copy** of the message gets sent to the address specified in the divert. Consequently non-exclusive diverts can be used to "snoop" on another address

Diverts can also be configured to have an optional filter. If specified then only matching messages will be diverted.

Diverts can be configured to apply a Transformer. If specified, all diverted messages will have the opportunity of being transformed by the Transformer.

Diverts are a very sophisticated concept, which when combined with bridges can be used to create interesting and complex routings. The set of diverts can be thought of as a type of _routing table_ for messages.

## Example step-by-step

In this example we will imagine a fictitious company which has two offices; one in London and another in New York.

The company accepts orders for it's products only at it's London office, and also generates price-updates for it's products, also only from it's London office. However only the New York office is interested in receiving price updates for New York products. Any prices for New York products need to be forwarded to the New York office.

There is an unreliable WAN linking the London and New York offices.

The company also requires a copy of any order received to be available to be inspected by management.

In order to achieve this, we will create a queue `orderQueue` on the London broker in to which orders arrive.

We will create a topic, `spyTopic` on the London server, and there will be two subscribers both in London.

We will create a _non-exclusive_ divert on the London broker which will siphon off a copy of each order received to the topic `spyTopic`.

Here's the xml config for that divert, from `broker.xml`

    <divert name="order-divert">
       <address>orders</address>
       <forwarding-address>spyTopic</forwarding-address>
       <exclusive>false</exclusive>
    </divert>

For the prices we will create a topic on the London server, `priceUpdates` to which all price updates are sent. We will create another topic on the New York broker `newYorkPriceUpdates` to which all New York price updates need to be forwarded.

Diverts can only be used to divert messages from one **local** address to another **local** address so we cannot divert directly to an address on another server.

Instead we divert to a local _store and forward queue_ they we define in the configuration. This is just a normal queue that we use for storing messages before forwarding to another node.

Here's the configuration for it:

    <address name="priceForwarding">
       <anycast>
          <queue name="priceForwarding"/>
       </anycast>
    </address>

Here's the configuration for the divert:

    <divert name="prices-divert">
       <address>priceUpdates</address>
       <forwarding-address>priceForwarding</forwarding-address>
       <filter string="office='New York'"/>
       <transformer-class-name>org.apache.activemq.artemis.jms.example.AddForwardingTimeTransformer</transformer-class-name>
       <exclusive>true</exclusive>
    </divert>

Note we specify a filter in the divert, so only New York prices get diverted. We also specify a Transformer class since we are going to insert a header in the message at divert time, recording the time the diversion happened.

And finally we define a bridge that moves messages from the local queue to the address on the New York server. Bridges move messages from queues to remote addresses and are ideal to use when the target broker may be stopped and started independently, and/or the network might be unreliable. Bridges guarantee once and only once delivery of messages from their source queues to their target addresses.

Here is the bridge configuration:

    <bridges>
       <bridge name="price-forward-bridge">
          <queue-name>priceForwarding</queue-name>
          <forwarding-address>newYorkPriceUpdates</forwarding-address>
          <reconnect-attempts>-1</reconnect-attempts>
          <static-connectors>
             <connector-ref>newyork-connector</connector-ref>
          </static-connectors>
	   </bridge>
    </bridges>