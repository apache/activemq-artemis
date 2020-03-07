# Message Redistribution Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example demonstrates message redistribution between queues with the same name deployed in different nodes of a cluster.

As demontrated in the clustered queue example, if queues with the same name are deployed on different nodes of a cluster, ActiveMQ Artemis can be configured to load balance messages between the nodes on the broker side.

However, if the consumer(s) on a particular node are closed, then messages in the queue at that node can appear to be stranded, since they have no local consumers.

If this is undesirable, ActiveMQ Artemis can be configured to **redistribute** messages from the node with no consumers, to nodes where there are consumers. If the consumers have JMS selectors set on them, then they will only be redistributed to nodes with consumers whose selectors match.

By default, message redistribution is disabled, but can be enabled by specifying some AddressSettings configuration in either `activemq-queues.xml` or `broker.xml`

Setting `redistribution-delay` to `0` will cause redistribution to occur immediately once there are no more matching consumers on a particular queue instance. Setting it to a positive value > 0 specifies a delay in milliseconds before attempting to redistribute. The delay is useful in the case that another consumer is likely to be created on the queue, to avoid unnecessary redistribution.

Here's the relevant snippet from the `activemq-queues.xml` configuration, which tells the broker to use a redistribution delay of `0` on any jms queues, i.e. any queues whose name starts with `jms.`

    <address-setting match="#">
       <redistribution-delay>0</redistribution-delay>
    </address-setting>

For more information on ActiveMQ Artemis load balancing, and clustering in general, please see the clustering section of the user manual.