# ActiveMQ Artemis Clustering with JGroups Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example demonstrates the working of a two node cluster using JGroups as the underlying topology broadcasting/discovery technique.

We deploy a queue on to the cluster, then create a consumer on the queue on each node, and we create a producer on only one of the nodes.

We then send some messages via the producer, and we verify that **both** consumers receive the sent messages in a round-robin fashion.

This example uses JNDI to lookup the JMS Queue and ConnectionFactory objects. If you prefer not to use JNDI, these could be instantiated directly.

To enable ActiveMQ Artemis to use JGroups you need to configure JGroups configuration file and make sure it is on the classpath by placing in the configuration directory, the file test-jgroups-file_ping.xml is the configuration used in this exaample

You then configure the jgroups file used by the broadcast and discovery groups in the configuration along with the channel name which you want this cluster to share.

    <broadcast-groups>
       <broadcast-group name="my-broadcast-group">
          <broadcast-period>5000</broadcast-period>
          <jgroups-file>test-jgroups-file_ping.xml</jgroups-file>
          <jgroups-channel>activemq_broadcast_channel</jgroups-channel>
          <connector-ref>netty-connector</connector-ref>
       </broadcast-group>
    </broadcast-groups>

    <discovery-groups>
       <discovery-group name="my-discovery-group">
          <jgroups-file>test-jgroups-file_ping.xml</jgroups-file>
          <jgroups-channel>activemq_broadcast_channel</jgroups-channel>
          <refresh-timeout>10000</refresh-timeout>
       </discovery-group>
    </discovery-groups>

For more information on ActiveMQ Artemis clustering in general, please see the "Clusters" chapter of the user manual.