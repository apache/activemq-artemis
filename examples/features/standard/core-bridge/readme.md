# Core Bridge Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example demonstrates a core bridge deployed on one server, which consumes messages from a local queue and forwards them to an address on a second server.

Core bridges are used to create message flows between any two ActiveMQ Artemis servers which are remotely separated. Core bridges are resilient and will cope with temporary connection failure allowing them to be an ideal choice for forwarding over unreliable connections, e.g. a WAN.

They can also be configured with an optional filter expression, and will only forward messages that match that filter.

Furthermore they can be configured to use an optional Transformer class. A user-defined Transformer class can be specified which is called at forwarding time. This gives the user the opportunity to transform the message in some ways, e.g. changing its properties or body

ActiveMQ Artemis also includes a **JMS Bridge**. This is similar to a core bridge, but uses the JMS API and can be used to bridge between any two JMS 1.1 compliant messaging systems. The core bridge is limited to bridging between ActiveMQ Artemis instances, but may provide better performance than the JMS bridge. The JMS bridge is covered in a separate example.

For more information on bridges, please see the ActiveMQ Artemis user manual.

In this example we will demonstrate a simple sausage factory for aardvarks.

We have a JMS queue on broker 0 named `sausage-factory`, and we have a JMS queue on broker 1 named `mincing-machine`

We want to forward any messages that are sent to the `sausage-factory` queue on broker 0, to the `mincing-machine` on broker 1.

We only want to make aardvark sausages, so we only forward messages where the property `name` is set to `aardvark`. It is known that other things, such are Sasquatches are also sent to the `sausage-factory` and we want to reject those.

Moreover it is known that Aardvarks normally wear blue hats, and it's important that we only make sausages using Aardvarks with green hats, so on the way we are going transform the property `hat` from `green` to `blue`.

Here's a snippet from `broker.xml` showing the bridge configuration

    <bridge name="my-bridge">
       <queue-name>jms.queue.sausage-factory</queue-name>
       <forwarding-address>jms.queue.mincing-machine</forwarding-address>
       <filter string="name='aardvark'"/>
       <transformer-class-name>org.apache.activemq.artemis.jms.example.HatColourChangeTransformer</transformer-class-name>
       <reconnect-attempts>-1</reconnect-attempts>
       <static-connectors>
          <connector-ref>remote-connector</connector-ref>
       </static-connectors>
    </bridge>