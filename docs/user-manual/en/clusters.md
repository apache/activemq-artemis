# Clusters

## Clusters Overview

Apache ActiveMQ clusters allow groups of Apache ActiveMQ servers to be grouped
together in order to share message processing load. Each active node in
the cluster is an active Apache ActiveMQ server which manages its own messages
and handles its own connections.

The cluster is formed by each node declaring *cluster connections* to
other nodes in the core configuration file `activemq-configuration.xml`.
When a node forms a cluster connection to another node, internally it
creates a *core bridge* (as described in [Core Bridges](core-bridges.md)) connection between it and
the other node, this is done transparently behind the scenes - you don't
have to declare an explicit bridge for each node. These cluster
connections allow messages to flow between the nodes of the cluster to
balance load.

Nodes can be connected together to form a cluster in many different
topologies, we will discuss a couple of the more common topologies later
in this chapter.

We'll also discuss client side load balancing, where we can balance
client connections across the nodes of the cluster, and we'll consider
message redistribution where Apache ActiveMQ will redistribute messages between
nodes to avoid starvation.

Another important part of clustering is *server discovery* where servers
can broadcast their connection details so clients or other servers can
connect to them with the minimum of configuration.

> **Warning**
>
> Once a cluster node has been configured it is common to simply copy
> that configuration to other nodes to produce a symmetric cluster.
> However, care must be taken when copying the Apache ActiveMQ files. Do not
> copy the Apache ActiveMQ *data* (i.e. the `bindings`, `journal`, and
> `large-messages` directories) from one node to another. When a node is
> started for the first time and initializes its journal files it also
> persists a special identifier to the `journal` directory. This id
> *must* be unique among nodes in the cluster or the cluster will not
> form properly.

## Server discovery

Server discovery is a mechanism by which servers can propagate their
connection details to:

-   Messaging clients. A messaging client wants to be able to connect to
    the servers of the cluster without having specific knowledge of
    which servers in the cluster are up at any one time.

-   Other servers. Servers in a cluster want to be able to create
    cluster connections to each other without having prior knowledge of
    all the other servers in the cluster.

This information, let's call it the Cluster Topology, is actually sent
around normal Apache ActiveMQ connections to clients and to other servers over
cluster connections. This being the case we need a way of establishing
the initial first connection. This can be done using dynamic discovery
techniques like
[UDP](http://en.wikipedia.org/wiki/User_Datagram_Protocol) and
[JGroups](http://www.jgroups.org/), or by providing a list of initial
connectors.

### Dynamic Discovery

Server discovery uses
[UDP](http://en.wikipedia.org/wiki/User_Datagram_Protocol) multicast or
[JGroups](http://www.jgroups.org/) to broadcast server connection
settings.

#### Broadcast Groups

A broadcast group is the means by which a server broadcasts connectors
over the network. A connector defines a way in which a client (or other
server) can make connections to the server. For more information on what
a connector is, please see [Configuring the Transport](configuring-transports.md).

The broadcast group takes a set of connector pairs, each connector pair
contains connection settings for a live and backup server (if one
exists) and broadcasts them on the network. Depending on which
broadcasting technique you configure the cluster, it uses either UDP or
JGroups to broadcast connector pairs information.

Broadcast groups are defined in the server configuration file
`activemq-configuration.xml`. There can be many broadcast groups per
Apache ActiveMQ server. All broadcast groups must be defined in a
`broadcast-groups` element.

Let's take a look at an example broadcast group from
`activemq-configuration.xml` that defines a UDP broadcast group:

    <broadcast-groups>
       <broadcast-group name="my-broadcast-group">
          <local-bind-address>172.16.9.3</local-bind-address>
          <local-bind-port>5432</local-bind-port>
          <group-address>231.7.7.7</group-address>
          <group-port>9876</group-port>
          <broadcast-period>2000</broadcast-period>
          <connector-ref connector-name="netty-connector"/>
       </broadcast-group>
    </broadcast-groups>

Some of the broadcast group parameters are optional and you'll normally
use the defaults, but we specify them all in the above example for
clarity. Let's discuss each one in turn:

-   `name` attribute. Each broadcast group in the server must have a
    unique name.

-   `local-bind-address`. This is the local bind address that the
    datagram socket is bound to. If you have multiple network interfaces
    on your server, you would specify which one you wish to use for
    broadcasts by setting this property. If this property is not
    specified then the socket will be bound to the wildcard address, an
    IP address chosen by the kernel. This is a UDP specific attribute.

-   `local-bind-port`. If you want to specify a local port to which the
    datagram socket is bound you can specify it here. Normally you would
    just use the default value of `-1` which signifies that an anonymous
    port should be used. This parameter is always specified in
    conjunction with `local-bind-address`. This is a UDP specific
    attribute.

-   `group-address`. This is the multicast address to which the data
    will be broadcast. It is a class D IP address in the range
    `224.0.0.0` to `239.255.255.255`, inclusive. The address `224.0.0.0`
    is reserved and is not available for use. This parameter is
    mandatory. This is a UDP specific attribute.

-   `group-port`. This is the UDP port number used for broadcasting.
    This parameter is mandatory. This is a UDP specific attribute.

-   `broadcast-period`. This is the period in milliseconds between
    consecutive broadcasts. This parameter is optional, the default
    value is `2000` milliseconds.

-   `connector-ref`. This specifies the connector and optional backup
    connector that will be broadcasted (see [Configuring the Transport](configuring-transports.md) for more information on
    connectors). The connector to be broadcasted is specified by the
    `connector-name` attribute.

Here is another example broadcast group that defines a JGroups broadcast
group:

    <broadcast-groups>
       <broadcast-group name="my-broadcast-group">
          <jgroups-file>test-jgroups-file_ping.xml</jgroups-file>
          <jgroups-channel>activemq_broadcast_channel</jgroups-channel>
          <broadcast-period>2000</broadcast-period>
        <connector-ref connector-name="netty-connector"/>
       </broadcast-group>
    </broadcast-groups>

To be able to use JGroups to broadcast, one must specify two attributes,
i.e. `jgroups-file` and `jgroups-channel`, as discussed in details as
following:

-   `jgroups-file` attribute. This is the name of JGroups configuration
    file. It will be used to initialize JGroups channels. Make sure the
    file is in the java resource path so that Apache ActiveMQ can load it.

-   `jgroups-channel` attribute. The name that JGroups channels connect
    to for broadcasting.

> **Note**
>
> The JGroups attributes (`jgroups-file` and `jgroups-channel`) and UDP
> specific attributes described above are exclusive of each other. Only
> one set can be specified in a broadcast group configuration. Don't mix
> them!

The following is an example of a JGroups file

    <config xmlns="urn:org:jgroups"
       xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
       xsi:schemaLocation="urn:org:jgroups http://www.jgroups.org/schema/JGroups-3.0.xsd">
       <TCP loopback="true"
          recv_buf_size="20000000"
          send_buf_size="640000"
          discard_incompatible_packets="true"
          max_bundle_size="64000"
          max_bundle_timeout="30"
          enable_bundling="true"
          use_send_queues="false"
          sock_conn_timeout="300"

          thread_pool.enabled="true"
          thread_pool.min_threads="1"
          thread_pool.max_threads="10"
          thread_pool.keep_alive_time="5000"
          thread_pool.queue_enabled="false"
          thread_pool.queue_max_size="100"
          thread_pool.rejection_policy="run"

          oob_thread_pool.enabled="true"
          oob_thread_pool.min_threads="1"
          oob_thread_pool.max_threads="8"
          oob_thread_pool.keep_alive_time="5000"
          oob_thread_pool.queue_enabled="false"
          oob_thread_pool.queue_max_size="100"
          oob_thread_pool.rejection_policy="run"/>

       <FILE_PING location="../file.ping.dir"/>
       <MERGE2 max_interval="30000"
          min_interval="10000"/>
       <FD_SOCK/>
       <FD timeout="10000" max_tries="5" />
       <VERIFY_SUSPECT timeout="1500"  />
       <BARRIER />
       <pbcast.NAKACK
          use_mcast_xmit="false"
          retransmit_timeout="300,600,1200,2400,4800"
          discard_delivered_msgs="true"/>
       <UNICAST timeout="300,600,1200" />
       <pbcast.STABLE stability_delay="1000" desired_avg_gossip="50000"
          max_bytes="400000"/>
       <pbcast.GMS print_local_addr="true" join_timeout="3000"
          view_bundling="true"/>
       <FC max_credits="2000000"
          min_threshold="0.10"/>
       <FRAG2 frag_size="60000"  />
       <pbcast.STATE_TRANSFER/>
       <pbcast.FLUSH timeout="0"/>
    </config>

As it shows, the file content defines a jgroups protocol stacks. If you
want Apache activemq to use this stacks for channel creation, you have to make
sure the value of `jgroups-file` in your broadcast-group/discovery-group
configuration to be the name of this jgroups configuration file. For
example if the above stacks configuration is stored in a file named
"jgroups-stacks.xml" then your `jgroups-file` should be like

    <jgroups-file>jgroups-stacks.xml</jgroups-file>

#### Discovery Groups

While the broadcast group defines how connector information is
broadcasted from a server, a discovery group defines how connector
information is received from a broadcast endpoint (a UDP multicast
address or JGroup channel).

A discovery group maintains a list of connector pairs - one for each
broadcast by a different server. As it receives broadcasts on the
broadcast endpoint from a particular server it updates its entry in the
list for that server.

If it has not received a broadcast from a particular server for a length
of time it will remove that server's entry from its list.

Discovery groups are used in two places in Apache ActiveMQ:

-   By cluster connections so they know how to obtain an initial
    connection to download the topology

-   By messaging clients so they know how to obtain an initial
    connection to download the topology

Although a discovery group will always accept broadcasts, its current
list of available live and backup servers is only ever used when an
initial connection is made, from then server discovery is done over the
normal Apache ActiveMQ connections.

> **Note**
>
> Each discovery group must be configured with broadcast endpoint (UDP
> or JGroups) that matches its broadcast group counterpart. For example,
> if broadcast is configured using UDP, the discovery group must also
> use UDP, and the same multicast address.

#### Defining Discovery Groups on the Server

For cluster connections, discovery groups are defined in the server side
configuration file `activemq-configuration.xml`. All discovery groups
must be defined inside a `discovery-groups` element. There can be many
discovery groups defined by Apache ActiveMQ server. Let's look at an example:

    <discovery-groups>
       <discovery-group name="my-discovery-group">
          <local-bind-address>172.16.9.7</local-bind-address>
          <group-address>231.7.7.7</group-address>
          <group-port>9876</group-port>
          <refresh-timeout>10000</refresh-timeout>
       </discovery-group>
    </discovery-groups>

We'll consider each parameter of the discovery group:

-   `name` attribute. Each discovery group must have a unique name per
    server.

-   `local-bind-address`. If you are running with multiple network
    interfaces on the same machine, you may want to specify that the
    discovery group listens only only a specific interface. To do this
    you can specify the interface address with this parameter. This
    parameter is optional. This is a UDP specific attribute.

-   `group-address`. This is the multicast IP address of the group to
    listen on. It should match the `group-address` in the broadcast
    group that you wish to listen from. This parameter is mandatory.
    This is a UDP specific attribute.

-   `group-port`. This is the UDP port of the multicast group. It should
    match the `group-port` in the broadcast group that you wish to
    listen from. This parameter is mandatory. This is a UDP specific
    attribute.

-   `refresh-timeout`. This is the period the discovery group waits
    after receiving the last broadcast from a particular server before
    removing that servers connector pair entry from its list. You would
    normally set this to a value significantly higher than the
    `broadcast-period` on the broadcast group otherwise servers might
    intermittently disappear from the list even though they are still
    broadcasting due to slight differences in timing. This parameter is
    optional, the default value is `10000` milliseconds (10 seconds).

Here is another example that defines a JGroups discovery group:

    <discovery-groups>
       <discovery-group name="my-broadcast-group">
          <jgroups-file>test-jgroups-file_ping.xml</jgroups-file>
          <jgroups-channel>activemq_broadcast_channel</jgroups-channel>
          <refresh-timeout>10000</refresh-timeout>
       </discovery-group>
    </discovery-groups>

To receive broadcast from JGroups channels, one must specify two
attributes, `jgroups-file` and `jgroups-channel`, as discussed in
details as following:

-   `jgroups-file` attribute. This is the name of JGroups configuration
    file. It will be used to initialize JGroups channels. Make sure the
    file is in the java resource path so that Apache ActiveMQ can load it.

-   `jgroups-channel` attribute. The name that JGroups channels connect
    to for receiving broadcasts.

> **Note**
>
> The JGroups attributes (`jgroups-file` and `jgroups-channel`) and UDP
> specific attributes described above are exclusive of each other. Only
> one set can be specified in a discovery group configuration. Don't mix
> them!

#### Discovery Groups on the Client Side

Let's discuss how to configure an Apache ActiveMQ client to use discovery to
discover a list of servers to which it can connect. The way to do this
differs depending on whether you're using JMS or the core API.

##### Configuring client discovery using JMS

If you're using JMS and you're using JNDI on the client to look up your
JMS connection factory instances then you can specify these parameters
in the JNDI context environment. e.g. in `jndi.properties`. Simply
ensure the host:port combination matches the group-address and
group-port from the corresponding `broadcast-group` on the server. Let's
take a look at an example:

    java.naming.factory.initial = org.apache.activemq.jndi.ActiveMQInitialContextFactory
    connectionFactory.myConnectionFactory=udp://231.7.7.7:9876

The element `discovery-group-ref` specifies the name of a discovery
group defined in `activemq-configuration.xml`.

When this connection factory is downloaded from JNDI by a client
application and JMS connections are created from it, those connections
will be load-balanced across the list of servers that the discovery
group maintains by listening on the multicast address specified in the
discovery group configuration.

If you're using JMS, but you're not using JNDI to lookup a connection
factory - you're instantiating the JMS connection factory directly then
you can specify the discovery group parameters directly when creating
the JMS connection factory. Here's an example:

``` java
final String groupAddress = "231.7.7.7";

final int groupPort = 9876;

ConnectionFactory jmsConnectionFactory =
ActiveMQJMSClient.createConnectionFactory(new DiscoveryGroupConfiguration(groupAddress, groupPort,
                       new UDPBroadcastGroupConfiguration(groupAddress, groupPort, null, -1)), JMSFactoryType.CF);

Connection jmsConnection1 = jmsConnectionFactory.createConnection();

Connection jmsConnection2 = jmsConnectionFactory.createConnection();
```

The `refresh-timeout` can be set directly on the
DiscoveryGroupConfiguration by using the setter method
`setDiscoveryRefreshTimeout()` if you want to change the default value.

There is also a further parameter settable on the
DiscoveryGroupConfiguration using the setter method
`setDiscoveryInitialWaitTimeout()`. If the connection factory is used
immediately after creation then it may not have had enough time to
received broadcasts from all the nodes in the cluster. On first usage,
the connection factory will make sure it waits this long since creation
before creating the first connection. The default value for this
parameter is `10000` milliseconds.

##### Configuring client discovery using Core

If you're using the core API to directly instantiate
`ClientSessionFactory` instances, then you can specify the discovery
group parameters directly when creating the session factory. Here's an
example:

``` java
final String groupAddress = "231.7.7.7";
final int groupPort = 9876;
ServerLocator factory = ActiveMQClient.createServerLocatorWithHA(new DiscoveryGroupConfiguration(groupAddress, groupPort,
                           new UDPBroadcastGroupConfiguration(groupAddress, groupPort, null, -1))));
ClientSessionFactory factory = locator.createSessionFactory();
ClientSession session1 = factory.createSession();
ClientSession session2 = factory.createSession();
```

The `refresh-timeout` can be set directly on the
DiscoveryGroupConfiguration by using the setter method
`setDiscoveryRefreshTimeout()` if you want to change the default value.

There is also a further parameter settable on the
DiscoveryGroupConfiguration using the setter method
`setDiscoveryInitialWaitTimeout()`. If the session factory is used
immediately after creation then it may not have had enough time to
received broadcasts from all the nodes in the cluster. On first usage,
the session factory will make sure it waits this long since creation
before creating the first session. The default value for this parameter
is `10000` milliseconds.

### Discovery using static Connectors

Sometimes it may be impossible to use UDP on the network you are using.
In this case its possible to configure a connection with an initial list
if possible servers. This could be just one server that you know will
always be available or a list of servers where at least one will be
available.

This doesn't mean that you have to know where all your servers are going
to be hosted, you can configure these servers to use the reliable
servers to connect to. Once they are connected there connection details
will be propagated via the server it connects to

#### Configuring a Cluster Connection

For cluster connections there is no extra configuration needed, you just
need to make sure that any connectors are defined in the usual manner,
(see [Configuring the Transport](configuring-transports.md) for more information on connectors). These are then referenced by
the cluster connection configuration.

#### Configuring a Client Connection

A static list of possible servers can also be used by a normal client.

##### Configuring client discovery using JMS

If you're using JMS and you're using JNDI on the client to look up your
JMS connection factory instances then you can specify these parameters
in the JNDI context environment in, e.g. `jndi.properties`:

    java.naming.factory.initial=org.apache.activemq.jndi.ActiveMQInitialContextFactory
    connectionFactory.myConnectionFactory=(tcp://myhost:61616,tcp://myhost2:61616)

The `connectionFactory.myConnectionFactory` contains a list of servers to use for the
connection factory. When this connection factory used client application
and JMS connections are created from it, those connections will be
load-balanced across the list of servers defined within the brackets `()`.
The brackets are expanded so the same query cab be appended after the last bracket for ease.

If you're using JMS, but you're not using JNDI to lookup a connection
factory - you're instantiating the JMS connection factory directly then
you can specify the connector list directly when creating the JMS
connection factory. Here's an example:

``` java
HashMap<String, Object> map = new HashMap<String, Object>();
map.put("host", "myhost");
map.put("port", "61616");
TransportConfiguration server1 = new TransportConfiguration(NettyConnectorFactory.class.getName(), map);
HashMap<String, Object> map2 = new HashMap<String, Object>();
map2.put("host", "myhost2");
map2.put("port", "61617");
TransportConfiguration server2 = new TransportConfiguration(NettyConnectorFactory.class.getName(), map2);

ActiveMQConnectionFactory cf = ActiveMQJMSClient.createConnectionFactoryWithHA(JMSFactoryType.CF, server1, server2);
```

##### Configuring client discovery using Core

If you are using the core API then the same can be done as follows:

``` java
HashMap<String, Object> map = new HashMap<String, Object>();
map.put("host", "myhost");
map.put("port", "61616");
TransportConfiguration server1 = new TransportConfiguration(NettyConnectorFactory.class.getName(), map);
HashMap<String, Object> map2 = new HashMap<String, Object>();
map2.put("host", "myhost2");
map2.put("port", "61617");
TransportConfiguration server2 = new TransportConfiguration(NettyConnectorFactory.class.getName(), map2);

ServerLocator locator = ActiveMQClient.createServerLocatorWithHA(server1, server2);
ClientSessionFactory factory = locator.createSessionFactory();
ClientSession session = factory.createSession();
```

## Server-Side Message Load Balancing

If cluster connections are defined between nodes of a cluster, then
Apache ActiveMQ will load balance messages arriving at a particular node from a
client.

Let's take a simple example of a cluster of four nodes A, B, C, and D
arranged in a *symmetric cluster* (described in Symmetrical Clusters section). We have a queue
called `OrderQueue` deployed on each node of the cluster.

We have client Ca connected to node A, sending orders to the server. We
have also have order processor clients Pa, Pb, Pc, and Pd connected to
each of the nodes A, B, C, D. If no cluster connection was defined on
node A, then as order messages arrive on node A they will all end up in
the `OrderQueue` on node A, so will only get consumed by the order
processor client attached to node A, Pa.

If we define a cluster connection on node A, then as ordered messages
arrive on node A instead of all of them going into the local
`OrderQueue` instance, they are distributed in a round-robin fashion
between all the nodes of the cluster. The messages are forwarded from
the receiving node to other nodes of the cluster. This is all done on
the server side, the client maintains a single connection to node A.

For example, messages arriving on node A might be distributed in the
following order between the nodes: B, D, C, A, B, D, C, A, B, D. The
exact order depends on the order the nodes started up, but the algorithm
used is round robin.

Apache ActiveMQ cluster connections can be configured to always blindly load
balance messages in a round robin fashion irrespective of whether there
are any matching consumers on other nodes, but they can be a bit
cleverer than that and also be configured to only distribute to other
nodes if they have matching consumers. We'll look at both these cases in
turn with some examples, but first we'll discuss configuring cluster
connections in general.

### Configuring Cluster Connections

Cluster connections group servers into clusters so that messages can be
load balanced between the nodes of the cluster. Let's take a look at a
typical cluster connection. Cluster connections are always defined in
`activemq-configuration.xml` inside a `cluster-connection` element.
There can be zero or more cluster connections defined per Apache ActiveMQ
server.

    <cluster-connections>
       <cluster-connection name="my-cluster">
          <address>jms</address>
          <connector-ref>netty-connector</connector-ref>
          <check-period>1000</check-period>
          <connection-ttl>5000</connection-ttl>
          <min-large-message-size>50000</min-large-message-size>
          <call-timeout>5000</call-timeout>
          <retry-interval>500</retry-interval>
          <retry-interval-multiplier>1.0</retry-interval-multiplier>
          <max-retry-interval>5000</max-retry-interval>
          <initial-connect-attempts>-1</initial-connect-attempts>
          <reconnect-attempts>-1</reconnect-attempts>
          <use-duplicate-detection>true</use-duplicate-detection>
          <forward-when-no-consumers>false</forward-when-no-consumers>
          <max-hops>1</max-hops>
          <confirmation-window-size>32000</confirmation-window-size>
          <call-failover-timeout>30000</call-failover-timeout>
          <notification-interval>1000</notification-interval>
          <notification-attempts>2</notification-attempts>
          <discovery-group-ref discovery-group-name="my-discovery-group"/>
       </cluster-connection>
    </cluster-connections>

In the above cluster connection all parameters have been explicitly
specified. The following shows all the available configuration options

-   `address` Each cluster connection only applies to addresses that
    match the specified address field. An address is matched on the
    cluster connection when it begins with the string specified in this
    field. The address field on a cluster connection also supports comma
    separated lists and an exclude syntax '!'. To prevent an address
    from being matched on this cluster connection, prepend a cluster
    connection address string with '!'.

    In the case shown above the cluster connection will load balance
    messages sent to addresses that start with `jms`. This cluster
    connection, will, in effect apply to all JMS queues and topics since
    they map to core queues that start with the substring "jms".

    The address can be any value and you can have many cluster
    connections with different values of `address`, simultaneously
    balancing messages for those addresses, potentially to different
    clusters of servers. By having multiple cluster connections on
    different addresses a single Apache ActiveMQ Server can effectively take
    part in multiple clusters simultaneously.

    Be careful not to have multiple cluster connections with overlapping
    values of `address`, e.g. "europe" and "europe.news" since this
    could result in the same messages being distributed between more
    than one cluster connection, possibly resulting in duplicate
    deliveries.

    Examples:

    -   'jms.eu'
        matches all addresses starting with 'jms.eu'
    -   '!jms.eu'
        matches all address except for those starting with 'jms.eu'
    -   'jms.eu.uk,jms.eu.de'
        matches all addresses starting with either 'jms.eu.uk' or
        'jms.eu.de'
    -   'jms.eu,!jms.eu.uk'
        matches all addresses starting with 'jms.eu' but not those
        starting with 'jms.eu.uk'

    Notes:

    -   Address exclusion will always takes precedence over address
        inclusion.
    -   Address matching on cluster connections does not support
        wild-card matching.

    This parameter is mandatory.

-   `connector-ref`. This is the connector which will be sent to other
    nodes in the cluster so they have the correct cluster topology.

    This parameter is mandatory.

-   `check-period`. The period (in milliseconds) used to check if the
    cluster connection has failed to receive pings from another server.
    Default is 30000.

-   `connection-ttl`. This is how long a cluster connection should stay
    alive if it stops receiving messages from a specific node in the
    cluster. Default is 60000.

-   `min-large-message-size`. If the message size (in bytes) is larger
    than this value then it will be split into multiple segments when
    sent over the network to other cluster members. Default is 102400.

-   `call-timeout`. When a packet is sent via a cluster connection and
    is a blocking call, i.e. for acknowledgements, this is how long it
    will wait (in milliseconds) for the reply before throwing an
    exception. Default is 30000.

-   `retry-interval`. We mentioned before that, internally, cluster
    connections cause bridges to be created between the nodes of the
    cluster. If the cluster connection is created and the target node
    has not been started, or say, is being rebooted, then the cluster
    connections from other nodes will retry connecting to the target
    until it comes back up, in the same way as a bridge does.

    This parameter determines the interval in milliseconds between retry
    attempts. It has the same meaning as the `retry-interval` on a
    bridge (as described in [Core Bridges](core-bridges.md)).

    This parameter is optional and its default value is `500`
    milliseconds.

-   `retry-interval-multiplier`. This is a multiplier used to increase
    the `retry-interval` after each reconnect attempt, default is 1.

-   `max-retry-interval`. The maximum delay (in milliseconds) for
    retries. Default is 2000.

-   `initial-connect-attempts`. The number of times the system will try
    to connect a node in the cluster initially. If the max-retry is
    achieved this node will be considered permanently down and the
    system will not route messages to this node. Default is -1 (infinite
    retries).

-   `reconnect-attempts`. The number of times the system will try to
    reconnect to a node in the cluster. If the max-retry is achieved
    this node will be considered permanently down and the system will
    stop routing messages to this node. Default is -1 (infinite
    retries).

-   `use-duplicate-detection`. Internally cluster connections use
    bridges to link the nodes, and bridges can be configured to add a
    duplicate id property in each message that is forwarded. If the
    target node of the bridge crashes and then recovers, messages might
    be resent from the source node. By enabling duplicate detection any
    duplicate messages will be filtered out and ignored on receipt at
    the target node.

    This parameter has the same meaning as `use-duplicate-detection` on
    a bridge. For more information on duplicate detection, please see [Duplicate Detection](duplicate-detection.md).
    Default is true.

-   `forward-when-no-consumers`. This parameter determines whether
    messages will be distributed round robin between other nodes of the
    cluster *regardless* of whether or not there are matching or indeed
    any consumers on other nodes.

    If this is set to `true` then each incoming message will be round
    robin'd even though the same queues on the other nodes of the
    cluster may have no consumers at all, or they may have consumers
    that have non matching message filters (selectors). Note that
    Apache ActiveMQ will *not* forward messages to other nodes if there are no
    *queues* of the same name on the other nodes, even if this parameter
    is set to `true`.

    If this is set to `false` then Apache ActiveMQ will only forward messages
    to other nodes of the cluster if the address to which they are being
    forwarded has queues which have consumers, and if those consumers
    have message filters (selectors) at least one of those selectors
    must match the message.

    Default is false.

-   `max-hops`. When a cluster connection decides the set of nodes to
    which it might load balance a message, those nodes do not have to be
    directly connected to it via a cluster connection. Apache ActiveMQ can be
    configured to also load balance messages to nodes which might be
    connected to it only indirectly with other Apache ActiveMQ servers as
    intermediates in a chain.

    This allows Apache ActiveMQ to be configured in more complex topologies and
    still provide message load balancing. We'll discuss this more later
    in this chapter.

    The default value for this parameter is `1`, which means messages
    are only load balanced to other Apache ActiveMQ serves which are directly
    connected to this server. This parameter is optional.

-   `confirmation-window-size`. The size (in bytes) of the window used
    for sending confirmations from the server connected to. So once the
    server has received `confirmation-window-size` bytes it notifies its
    client, default is 1048576. A value of -1 means no window.

-   `producer-window-size`. The size for producer flow control over cluster connection.
     it's by default disabled through the cluster connection bridge but you may want
     to set a value if you are using really large messages in cluster. A value of -1 means no window.

-   `call-failover-timeout`. Similar to `call-timeout` but used when a
    call is made during a failover attempt. Default is -1 (no timeout).

-   `notification-interval`. How often (in milliseconds) the cluster
    connection should broadcast itself when attaching to the cluster.
    Default is 1000.

-   `notification-attempts`. How many times the cluster connection
    should broadcast itself when connecting to the cluster. Default is
    2.

-   `discovery-group-ref`. This parameter determines which discovery
    group is used to obtain the list of other servers in the cluster
    that this cluster connection will make connections to.

Alternatively if you would like your cluster connections to use a static
list of servers for discovery then you can do it like this.

    <cluster-connection name="my-cluster">
       ...
       <static-connectors>
          <connector-ref>server0-connector</connector-ref>
          <connector-ref>server1-connector</connector-ref>
       </static-connectors>
    </cluster-connection>

Here we have defined 2 servers that we know for sure will that at least
one will be available. There may be many more servers in the cluster but
these will; be discovered via one of these connectors once an initial
connection has been made.

### Cluster User Credentials

When creating connections between nodes of a cluster to form a cluster
connection, Apache ActiveMQ uses a cluster user and cluster password which is
defined in `activemq-configuration.xml`:

    <cluster-user>ACTIVEMQ.CLUSTER.ADMIN.USER</cluster-user>
    <cluster-password>CHANGE ME!!</cluster-password>

> **Warning**
>
> It is imperative that these values are changed from their default, or
> remote clients will be able to make connections to the server using
> the default values. If they are not changed from the default, Apache ActiveMQ
> will detect this and pester you with a warning on every start-up.

## Client-Side Load balancing

With Apache ActiveMQ client-side load balancing, subsequent sessions created
using a single session factory can be connected to different nodes of
the cluster. This allows sessions to spread smoothly across the nodes of
a cluster and not be "clumped" on any particular node.

The load balancing policy to be used by the client factory is
configurable. Apache ActiveMQ provides four out-of-the-box load balancing
policies, and you can also implement your own and use that.

The out-of-the-box policies are

-   Round Robin. With this policy the first node is chosen randomly then
    each subsequent node is chosen sequentially in the same order.

    For example nodes might be chosen in the order B, C, D, A, B, C, D,
    A, B or D, A, B, C, D, A, B, C, D or C, D, A, B, C, D, A, B, C.

    Use
    `org.apache.activemq.api.core.client.loadbalance.RoundRobinConnectionLoadBalancingPolicy`
    as the `<connection-load-balancing-policy-class-name>`.

-   Random. With this policy each node is chosen randomly.

    Use
    `org.apache.activemq.api.core.client.loadbalance.RandomConnectionLoadBalancingPolicy`
    as the `<connection-load-balancing-policy-class-name>`.

-   Random Sticky. With this policy the first node is chosen randomly
    and then re-used for subsequent connections.

    Use
    `org.apache.activemq.api.core.client.loadbalance.RandomStickyConnectionLoadBalancingPolicy`
    as the `<connection-load-balancing-policy-class-name>`.

-   First Element. With this policy the "first" (i.e. 0th) node is
    always returned.

    Use
    `org.apache.activemq.api.core.client.loadbalance.FirstElementConnectionLoadBalancingPolicy`
    as the `<connection-load-balancing-policy-class-name>`.

You can also implement your own policy by implementing the interface
`org.apache.activemq.api.core.client.loadbalance.ConnectionLoadBalancingPolicy`

Specifying which load balancing policy to use differs whether you are
using JMS or the core API. If you don't specify a policy then the
default will be used which is
`org.apache.activemq.api.core.client.loadbalance.RoundRobinConnectionLoadBalancingPolicy`.

If you're using JMS and you're using JNDI on the client to look up your
JMS connection factory instances then you can specify these parameters
in the JNDI context environment in, e.g. `jndi.properties`, to specify
the load balancing policy directly:

    java.naming.factory.initial=org.apache.activemq.jndi.ActiveMQInitialContextFactory
    connection.myConnectionFactory=tcp://localhost:61616?loadBalancingPolicyClassName=org.apache.activemq.api.core.client.loadbalance.RandomConnectionLoadBalancingPolicy

The above example would instantiate a JMS connection factory that uses
the random connection load balancing policy.

If you're using JMS but you're instantiating your connection factory
directly on the client side then you can set the load balancing policy
using the setter on the `ActiveMQConnectionFactory` before using it:

``` java
ConnectionFactory jmsConnectionFactory = ActiveMQJMSClient.createConnectionFactory(...);
jmsConnectionFactory.setLoadBalancingPolicyClassName("com.acme.MyLoadBalancingPolicy");
```

If you're using the core API, you can set the load balancing policy
directly on the `ServerLocator` instance you are using:

``` java
ServerLocator locator = ActiveMQClient.createServerLocatorWithHA(server1, server2);
locator.setLoadBalancingPolicyClassName("com.acme.MyLoadBalancingPolicy");
```

The set of servers over which the factory load balances can be
determined in one of two ways:

-   Specifying servers explicitly

-   Using discovery.

## Specifying Members of a Cluster Explicitly

Sometimes you want to explicitly define a cluster more explicitly, that
is control which server connect to each other in the cluster. This is
typically used to form non symmetrical clusters such as chain cluster or
ring clusters. This can only be done using a static list of connectors
and is configured as follows:

    <cluster-connection name="my-cluster">
       <address>jms</address>
       <connector-ref>netty-connector</connector-ref>
       <retry-interval>500</retry-interval>
       <use-duplicate-detection>true</use-duplicate-detection>
       <forward-when-no-consumers>true</forward-when-no-consumers>
       <max-hops>1</max-hops>
       <static-connectors allow-direct-connections-only="true">
          <connector-ref>server1-connector</connector-ref>
       </static-connectors>
    </cluster-connection>

In this example we have set the attribute
`allow-direct-connections-only` which means that the only server that
this server can create a cluster connection to is server1-connector.
This means you can explicitly create any cluster topology you want.

## Message Redistribution

Another important part of clustering is message redistribution. Earlier
we learned how server side message load balancing round robins messages
across the cluster. If `forward-when-no-consumers` is false, then
messages won't be forwarded to nodes which don't have matching
consumers, this is great and ensures that messages don't arrive on a
queue which has no consumers to consume them, however there is a
situation it doesn't solve: What happens if the consumers on a queue
close after the messages have been sent to the node? If there are no
consumers on the queue the message won't get consumed and we have a
*starvation* situation.

This is where message redistribution comes in. With message
redistribution Apache ActiveMQ can be configured to automatically
*redistribute* messages from queues which have no consumers back to
other nodes in the cluster which do have matching consumers.

Message redistribution can be configured to kick in immediately after
the last consumer on a queue is closed, or to wait a configurable delay
after the last consumer on a queue is closed before redistributing. By
default message redistribution is disabled.

Message redistribution can be configured on a per address basis, by
specifying the redistribution delay in the address settings, for more
information on configuring address settings, please see [Queue Attributes](queue-attributes.md).

Here's an address settings snippet from `activemq-configuration.xml`
showing how message redistribution is enabled for a set of queues:

    <address-settings>
       <address-setting match="jms.#">
          <redistribution-delay>0</redistribution-delay>
       </address-setting>
    </address-settings>

The above `address-settings` block would set a `redistribution-delay` of
`0` for any queue which is bound to an address that starts with "jms.".
All JMS queues and topic subscriptions are bound to addresses that start
with "jms.", so the above would enable instant (no delay) redistribution
for all JMS queues and topic subscriptions.

The attribute `match` can be an exact match or it can be a string that
conforms to the Apache ActiveMQ wildcard syntax (described in [Wildcard Syntax](wildcard-syntax.md)).

The element `redistribution-delay` defines the delay in milliseconds
after the last consumer is closed on a queue before redistributing
messages from that queue to other nodes of the cluster which do have
matching consumers. A delay of zero means the messages will be
immediately redistributed. A value of `-1` signifies that messages will
never be redistributed. The default value is `-1`.

It often makes sense to introduce a delay before redistributing as it's
a common case that a consumer closes but another one quickly is created
on the same queue, in such a case you probably don't want to
redistribute immediately since the new consumer will arrive shortly.

## Cluster topologies

Apache ActiveMQ clusters can be connected together in many different
topologies, let's consider the two most common ones here

### Symmetric cluster

A symmetric cluster is probably the most common cluster topology.

With a symmetric cluster every node in the cluster is connected to every
other node in the cluster. In other words every node in the cluster is
no more than one hop away from every other node.

To form a symmetric cluster every node in the cluster defines a cluster
connection with the attribute `max-hops` set to `1`. Typically the
cluster connection will use server discovery in order to know what other
servers in the cluster it should connect to, although it is possible to
explicitly define each target server too in the cluster connection if,
for example, UDP is not available on your network.

With a symmetric cluster each node knows about all the queues that exist
on all the other nodes and what consumers they have. With this knowledge
it can determine how to load balance and redistribute messages around
the nodes.

Don't forget [this warning](#copy-warning) when creating a symmetric
cluster.

### Chain cluster

With a chain cluster, each node in the cluster is not connected to every
node in the cluster directly, instead the nodes form a chain with a node
on each end of the chain and all other nodes just connecting to the
previous and next nodes in the chain.

An example of this would be a three node chain consisting of nodes A, B
and C. Node A is hosted in one network and has many producer clients
connected to it sending order messages. Due to corporate policy, the
order consumer clients need to be hosted in a different network, and
that network is only accessible via a third network. In this setup node
B acts as a mediator with no producers or consumers on it. Any messages
arriving on node A will be forwarded to node B, which will in turn
forward them to node C where they can get consumed. Node A does not need
to directly connect to C, but all the nodes can still act as a part of
the cluster.

To set up a cluster in this way, node A would define a cluster
connection that connects to node B, and node B would define a cluster
connection that connects to node C. In this case we only want cluster
connections in one direction since we're only moving messages from node
A-\>B-\>C and never from C-\>B-\>A.

For this topology we would set `max-hops` to `2`. With a value of `2`
the knowledge of what queues and consumers that exist on node C would be
propagated from node C to node B to node A. Node A would then know to
distribute messages to node B when they arrive, even though node B has
no consumers itself, it would know that a further hop away is node C
which does have consumers.

### Scaling Down

Apache ActiveMQ supports scaling down a cluster with no message loss (even for
non-durable messages). This is especially useful in certain environments
(e.g. the cloud) where the size of a cluster may change relatively
frequently. When scaling up a cluster (i.e. adding nodes) there is no
risk of message loss, but when scaling down a cluster (i.e. removing
nodes) the messages on those nodes would be lost unless the broker sent
them to another node in the cluster. Apache ActiveMQ can be configured to do
just that.

The simplest way to enable this behavior is to set `scale-down` to
`true`. If the server is clustered and `scale-down` is `true` then when
the server is shutdown gracefully (i.e. stopped without crashing) it
will find another node in the cluster and send *all* of its messages
(both durable and non-durable) to that node. The messages are processed
in order and go to the *back* of the respective queues on the other node
(just as if the messages were sent from an external client for the first
time).

If more control over where the messages go is required then specify
`scale-down-group-name`. Messages will only be sent to another node in
the cluster that uses the same `scale-down-group-name` as the server
being shutdown.

> **Warning**
>
> If cluster nodes are grouped together with different
> `scale-down-group-name` values beware. If all the nodes in a single
> group are shut down then the messages from that node/group will be
> lost.

If the server is using multiple `cluster-connection` then use
`scale-down-clustername` to identify the name of the
`cluster-connection` which should be used for scaling down.
