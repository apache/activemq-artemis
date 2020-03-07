# JMS Clustered Grouping Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example demonstrates how to ensure strict ordering across a cluster using clustered message grouping

We create 3 nodes each with a grouping message handler, one with a Local handler and 2 with a Remote handler.

The local handler acts as an arbitrator for the 2 remote handlers, holding the information on routes and communicating the routing info with the remote handlers on the other 2 nodes

We then send some messages to each node with the same group id set and ensure the same consumer receives all of them

Here's the relevant snippet from the broker configuration that has the local handler

    <cluster-connections>
       <cluster-connection name="my-cluster">
          <connector-ref>netty-connector</connector-ref>
          <retry-interval>500</retry-interval>
          <use-duplicate-detection>true</use-duplicate-detection>
          <message-load-balancing>STRICT</message-load-balancing>
          <max-hops>1</max-hops>
          <discovery-group-ref discovery-group-name="my-discovery-group"/>
       </cluster-connection>
    </cluster-connections>

    <grouping-handler name="my-grouping-handler">
       <type>LOCAL</type>
       <address>jms</address>
       <timeout>5000</timeout>
    </grouping-handler>

Here's the relevant snippet from the broker configuration that has the remote handlers

    <cluster-connections>
       <cluster-connection name="my-cluster">
          <retry-interval>500</retry-interval>
          <use-duplicate-detection>true</use-duplicate-detection>
          <message-load-balancing>STRICT</message-load-balancing>
          <max-hops>1</max-hops>
          <discovery-group-ref discovery-group-name="my-discovery-group"/>
       </cluster-connection>
    </cluster-connections>

    <grouping-handler name="my-grouping-handler">
       <type>REMOTE</type>
       <address>jms</address>
       <timeout>5000</timeout>
    </grouping-handler>