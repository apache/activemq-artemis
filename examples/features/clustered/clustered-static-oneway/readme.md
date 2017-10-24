# JMS Load Balanced Static Clustered One Way Queue Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example demonstrates a JMS queue deployed on three different nodes. The three nodes are configured to form a one way cluster from a _static_ list of nodes.

A one way cluster is different from a symmetrical cluster in that each node is only connected to one another node in a chain type fashion, so broker 0 -> broker 1 -> broker 2

We then create a consumer on the queue on each node, and we create a producer on only one of the nodes.

We then send some messages via the producer, and we verify that **all** consumers receive the sent messages in a round-robin fashion.

In other words, ActiveMQ Artemis **load balances** the sent messages across all consumers on the cluster

This example uses JNDI to lookup the JMS Queue and ConnectionFactory objects. If you prefer not to use JNDI, these could be instantiated directly.

Here's the relevant snippet from the broker configuration, which tells the broker to form a one way cluster between the three nodes and to load balance the messages between the nodes. Note that we have set _allow-direct-connections-only_ to true, this means that this broker will only ever connect the address's specified in the list of connectors. ALso notice that _max-hops_ is 2, this is because broker 0 is not directly connected to broker 2, 2 hops in fact, so we allow any updates from servers up to 2 hops away

    <cluster-connection name="my-cluster">
       <connector-ref>netty-connector</connector-ref>
       <retry-interval>500</retry-interval>
       <use-duplicate-detection>true</use-duplicate-detection>
       <message-load-balancing>STRICT</message-load-balancing>
       <max-hops>2</max-hops>
       <static-connectors allow-direct-connections-only="true">
           <connector-ref>server1-connector</connector-ref>
        </static-connectors>
    </cluster-connection>

For more information on ActiveMQ Artemis load balancing, and clustering in general, please see the clustering section of the user manual.