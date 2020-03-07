# OpenWire Virtual Topic Mapping Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start
and create the broker manually.

Using ActiveMQ 5.x virtual topics, messages that are sent to a virtual topic can be consumed from a set of backing queues.
This is similar to using an Artemis Address with a multicast binding and consuming from directly from the underlying
queue using a FQQN (Fully Qualified Queue Name).

In ActiveMQ 5.x the relation between the virtual topic and its backing queues is established by following a naming convention.
For more details on Virtual Topics please see http://activemq.apache.org/virtual-destinations.html

This example shows you how to map a virtual topic naming convention (from ActiveMQ 5.x) to use the Artemis Address model .
The Artemis broker is configured to recognise the Virtual Topic Naming convention, using `virtualTopicConsumerWildcards`
acceptor parameter and the consumer will be mapped internally to consume from the appropriate FQQN rather than the specified
Address.

The example sends a message to a topic (using openwire protocol) and an openwire consumer listens on the backing queue
using the ActiveMQ 5.x virtual topic naming convention. Due to the acceptor url parameter `virtualTopicConsumerWildcards`,
(see below), Artemis maps the consumer consuming from `Consumer.A.VirtualTopic.Orders` to actually consume from
FQQN of `VirtualTopic.Orders::Consumer.A.VirtualTopic.Orders`


```xml
<acceptor name="artemis">tcp://0.0.0.0:61616?virtualTopicConsumerWildcards=Consumer.*.%3E%3B2</acceptor>
```

