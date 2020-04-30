# JMS Symmetric Cluster Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This examples demonstrates a **symmetric cluster** set-up with ActiveMQ Artemis.

ActiveMQ Artemis has extremely flexible clustering which allows you to set-up servers in many different topologies.

The most common topology that you'll perhaps be familiar with if you are used to application broker clustering is a **symmetric cluster**.

With a symmetric cluster, the cluster is homogeneous, i.e. each node is configured the same as every other node, and every node is connected to every other node in the cluster.

By connecting node in such a way, we can, from a JMS point of view, give the impression of distributed JMS queues and topics.

The configuration used in this example is very similar to the configuration used by ActiveMQ when installed as a clustered profile in JBoss Application Server.

To set up ActiveMQ Artemis to form a symmetric cluster we simply need to mark each broker as `clustered` and we need to define a `cluster-connection` in `broker.xml`.

The `cluster-connection` tells the nodes what other nodes to make connections to. With a `cluster-connection` each node that we connect to can either be specified indivually, or we can use UDP discovery to find out what other nodes are in the cluster.

Using UDP discovery makes configuration simpler since we don't have to know what nodes are available at any one time.

Here's the relevant snippet from the broker configuration, which tells the broker to form a cluster with the other nodes:

    <cluster-connection name="my-cluster">
      <connector-ref>netty-connector</connector-ref>
	   <retry-interval>500</retry-interval>
	   <use-duplicate-detection>true</use-duplicate-detection>
	   <message-load-balancing>STRICT</message-load-balancing>
	   <max-hops>1</max-hops>
	   <discovery-group-ref discovery-group-name="my-discovery-group"/>
    </cluster-connection>

In this example we create a symmetric cluster of six live nodes.

In this example will we will demonstrate this by deploying a JMS topic and Queue on all nodes of the cluster , sending messages to the queue and topic from different nodes, and verifying messages are received correctly by consumers on different nodes.

For more information on configuring ActiveMQ Artemis clustering in general, please see the clustering section of the user manual.