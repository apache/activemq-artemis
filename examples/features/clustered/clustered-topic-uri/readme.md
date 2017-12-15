# JMS Clustered Topic URI Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example demonstrates a JMS Topic deployed on two different nodes. The two nodes are configured to form a cluster.

We then create a subscriber on the topic on each node, and we create a producer on only one of the nodes.

We then send some messages via the producer, and we verify that **both** subscribers receive all the sent messages.

A JMS Topic is an example of **publish-subscribe** messaging where all subscribers receive all the messages sent to the topic (assuming they have no message selectors).

This example uses JNDI to lookup the JMS Queue and ConnectionFactory objects. If you prefer not to use JNDI, these could be instantiated directly.

Here's the relevant snippet from the broker configuration, which tells the broker to form a cluster between the two nodes and to load balance the messages between the nodes.

This example differs from different-topic as it will use an URI to define the cluster connection.

    <cluster-connection-uri name="my-cluster" address="uri="multicast://my-discovery-group?messageLoadBalancingType=STRICT;retryInterval=500;connectorName=netty-connector;maxHops=1"/>

For more information on ActiveMQ Artemis load balancing, and clustering in general, please see the clustering section of the user manual.