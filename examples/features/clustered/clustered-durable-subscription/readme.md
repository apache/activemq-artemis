# JMS Clustered Durable Subscription Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example demonstrates a clustered JMS durable subscription. Normally durable subscriptions exist on a single node and can only have one subscriber at any one time, however, with ActiveMQ Artemis it's possible to create durable subscription instances with the same name and client-id on different nodes of the cluster, and consume from them simultaneously. This allows the work of processing messages from a durable subscription to be spread across the cluster in a similar way to how JMS Queues can be load balanced across the cluster

In this example we first configure the two nodes to form a cluster, then we then create a durable subscriber with the same name and client-id on both nodes, and we create a producer on only one of the nodes.

We then send some messages via the producer, and we verify that the messages are round robin'd between the two subscription instances. Note that each durable subscription instance with the same name and client-id **does not** receive its own copy of the messages. This is because the instances on different nodes form a single "logical" durable subscription, in the same way multiple JMS Queue instances on different nodes form a single "local" JMS Queue

This example uses JNDI to lookup the JMS Queue and ConnectionFactory objects. If you prefer not to use JNDI, these could be instantiated directly.

Here's the relevant snippet from the broker configuration, which tells the broker to form a cluster between the two nodes and to load balance the messages between the nodes.

The cli create method will define a similar section by default if you use `--clustered` as a parameter

    <cluster-connection name="my-cluster">
        <retry-interval>500</retry-interval>
        <use-duplicate-detection>true</use-duplicate-detection>
        <message-load-balancing>STRICT</message-load-balancing>
        <max-hops>1</max-hops>
        <discovery-group-ref discovery-group-name="my-discovery-group"/>
    </cluster-connection>

For more information on ActiveMQ Artemis load balancing, and clustering in general, please see the "Clusters" chapter of the user manual.