# JMS Load Balanced Clustered Queue Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example demonstrates a JMS queue deployed on two different nodes. The two nodes are configured to form a cluster.

We then create a consumer on the queue on each node, and we create a producer on only one of the nodes.

We then send some messages via the producer, and we verify that **both** consumers receive the sent messages in a round-robin fashion.

In other words, ActiveMQ Artemis **load balances** the sent messages across all consumers on the cluster

This example uses JNDI to lookup the JMS Queue and ConnectionFactory objects. If you prefer not to use JNDI, these could be instantiated directly.

Here's the relevant snippet from the broker configuration, which tells the broker to form a cluster between the two nodes and to load balance the messages between the nodes.

    <cluster-connection name="my-cluster">
       <connector-ref>netty-connector</connector-ref>
       <retry-interval>500</retry-interval>
       <use-duplicate-detection>true</use-duplicate-detection>
       <message-load-balancing>STRICT</message-load-balancing>
       <max-hops>1</max-hops>
       <discovery-group-ref discovery-group-name="my-discovery-group"/>
    </cluster-connection>

For more information on ActiveMQ Artemis load balancing, and clustering in general, please see the clustering section of the user manual.