Configuration Reference
=======================

This section is a quick index for looking up configuration. Click on the
element name to go to the specific chapter.

Server Configuration
====================

activemq-configuration.xml
--------------------------

This is the main core server configuration file which contains to elements
'core' and 'jms'.
The 'core' element contains the main server configuration while the 'jms'
element is used by the server side JMS service to load JMS Queues, Topics

# The core configuration


<table summary="Server Configuration" border="1">
    <colgroup>
        <col/>
        <col/>
        <col/>
        <col/>
    </colgroup>
    <thead>
    <tr>
        <th>Element Name</th>
        <th>Element Type</th>
        <th>Description</th>
        <th>Default</th>
    </tr>
    </thead>
    <tbody>
    <tr>
        <td>
            <a href="configuring-transports.html"
               title="16.1. Understanding Acceptors">acceptors</a>
        </td>
        <td>Sequence of &lt;acceptor/&gt;</td>
        <td>a list of remoting acceptors to create</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="configuring-transports.html"
               title="16.1. Understanding Acceptors">acceptors.acceptor</a>
        </td>
        <td>Complex element</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="configuring-transports.html"
               title="16.1. Understanding Acceptors">acceptors.acceptor.name (attribute)</a>
        </td>
        <td>xsd:string</td>
        <td>Name of the acceptor</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="configuring-transports.html"
               title="16.1. Understanding Acceptors">acceptors.acceptor.factory-class</a>
        </td>
        <td>xsd:string</td>
        <td>Name of the AcceptorFactory implementation</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="configuring-transports.html"
               title="16.1. Understanding Acceptors">acceptors.acceptor.param</a>
        </td>
        <td>Complex element</td>
        <td>A key-value pair used to configure the acceptor. An acceptor can have many param</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="configuring-transports.html"
               title="16.1. Understanding Acceptors">acceptors.acceptor.param.key (required attribute)</a>
        </td>
        <td>xsd:string</td>
        <td>Key of a configuration parameter</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="configuring-transports.html"
               title="16.1. Understanding Acceptors">acceptors.acceptor.param.value (required attribute)</a>
        </td>
        <td>xsd:string</td>
        <td>Value of a configuration parameter</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="queue-attributes.html"
               title="25.3. Configuring Queues Via Address Settings">address-settings</a>
        </td>
        <td>Sequence of &lt;address-setting/&gt;</td>
        <td>a list of address settings</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="queue-attributes.html"
               title="25.3. Configuring Queues Via Address Settings">address-settings.address-setting</a>
        </td>
        <td>Complex element</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="queue-attributes.html"
               title="25.3. Configuring Queues Via Address Settings">address-settings.address-setting.match (required
                attribute)</a>
        </td>
        <td>xsd:string</td>
        <td>XXX</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="undelivered-messages.html"
               title="21.2.1. Configuring Dead Letter Addresses">address-settings.address-setting.dead-letter-address</a>
        </td>
        <td>xsd:string</td>
        <td>the address to send dead messages to</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="message-expiry.html"
               title="22.2. Configuring Expiry Addresses">address-settings.address-setting.expiry-address</a>
        </td>
        <td>xsd:string</td>
        <td>the address to send expired messages to</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="queue-attributes.html"
               title="25.3. Configuring Queues Via Address Settings">address-settings.address-setting.expiry-delay</a>
        </td>
        <td>xsd:long</td>
        <td>Overrides the expiration time for messages using the default value for expiration time. "-1" disables this
            setting.
        </td>
        <td>-1</td>
    </tr>
    <tr>
        <td>
            <a href="undelivered-messages.html"
               title="21.1.1. Configuring Delayed Redelivery">address-settings.address-setting.redelivery-delay</a>
        </td>
        <td>xsd:long</td>
        <td>the time (in ms) to wait before redelivering a cancelled message.</td>
        <td>0</td>
    </tr>
    <tr>
        <td>
            <a href="queue-attributes.html"
               title="25.3. Configuring Queues Via Address Settings">address-settings.address-setting.redelivery-delay-multiplier</a>
        </td>
        <td>xsd:double</td>
        <td>multipler to apply to the "redelivery-delay"</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="queue-attributes.html"
               title="25.3. Configuring Queues Via Address Settings">address-settings.address-setting.max-redelivery-delay</a>
        </td>
        <td>xsd:long</td>
        <td>Maximum value for the redelivery-delay</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="undelivered-messages.html"
               title="21.2.1. Configuring Dead Letter Addresses">address-settings.address-setting.max-delivery-attempts</a>
        </td>
        <td>xsd:int</td>
        <td>how many times to attempt to deliver a message before sending to dead letter address</td>
        <td>10</td>
    </tr>
    <tr>
        <td>
            <a href="paging.html" title="Chapter 24. Paging">address-settings.address-setting.max-size-bytes</a>
        </td>
        <td>xsd:long</td>
        <td>the maximum size (in bytes) to use in paging for an address (-1 means no limits)</td>
        <td>-1</td>
    </tr>
    <tr>
        <td>
            <a href="paging.html" title="Chapter 24. Paging">address-settings.address-setting.page-size-bytes</a>
        </td>
        <td>xsd:long</td>
        <td>the page size (in bytes) to use for an address</td>
        <td>10485760 (10 * 1024 * 1024)</td>
    </tr>
    <tr>
        <td>
            <a href="paging.html" title="Chapter 24. Paging">address-settings.address-setting.page-max-cache-size</a>
        </td>
        <td>xsd:int</td>
        <td>Number of paging files to cache in memory to avoid IO during paging navigation</td>
        <td>5</td>
    </tr>
    <tr>
        <td>
            <a href="queue-attributes.html"
               title="25.3. Configuring Queues Via Address Settings">address-settings.address-setting.address-full-policy</a>
        </td>
        <td>DROP|FAIL|PAGE|BLOCK</td>
        <td>what happens when an address where "max-size-bytes" is specified becomes full</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="queue-attributes.html"
               title="25.3. Configuring Queues Via Address Settings">address-settings.address-setting.message-counter-history-day-limit</a>
        </td>
        <td>xsd:int</td>
        <td>how many days to keep message counter history for this address</td>
        <td>0 (days)</td>
    </tr>
    <tr>
        <td>
            <a href="last-value-queues.html" title="Chapter 27. Last-Value Queues">address-settings.address-setting.last-value-queue</a>
        </td>
        <td>xsd:boolean</td>
        <td>whether to treat the queue as a last value queue</td>
        <td>false</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">address-settings.address-setting.redistribution-delay</a>
        </td>
        <td>xsd:long</td>
        <td>how long (in ms) to wait after the last consumer is closed on a queue before redistributing messages.</td>
        <td>-1</td>
    </tr>
    <tr>
        <td>
            <a href="queue-attributes.html"
               title="25.3. Configuring Queues Via Address Settings">address-settings.address-setting.send-to-dla-on-no-route</a>
        </td>
        <td>xsd:boolean</td>
        <td>if there are no queues matching this address, whether to forward message to DLA (if it exists for this
            address)
        </td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="ha.html"
               title="39.1.4. Failing Back to live Server">allow-failback</a>
        </td>
        <td>xsd:boolean</td>
        <td>Whether a server will automatically stop when a another places a request to take over its place. The use
            case is when a regular server stops and its backup takes over its duties, later the main server restarts and
            requests the server (the former backup) to stop operating.
        </td>
        <td>true</td>
    </tr>
    <tr>
        <td>
            <a href="connection-ttl.html"
               title="17.3. Configuring Asynchronous Connection Execution">async-connection-execution-enabled</a>
        </td>
        <td>xsd:boolean</td>
        <td>Should incoming packets on the server be handed off to a thread from the thread pool for processing or
            should they be handled on the remoting thread?
        </td>
        <td>true</td>
    </tr>
    <tr>
        <td>
            <a href="persistence.html"
               title="15.1. Configuring the bindings journal">bindings-directory</a>
        </td>
        <td>xsd:string</td>
        <td>the directory to store the persisted bindings to</td>
        <td>data/bindings</td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html" title="Chapter 36. Core Bridges">bridges</a>
        </td>
        <td>Sequence of &lt;bridge/&gt;</td>
        <td>a list of bridges to create</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html" title="Chapter 36. Core Bridges">bridges.bridge</a>
        </td>
        <td>Complex element</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html" title="Chapter 36. Core Bridges">bridges.bridge.name (required
                attribute)</a>
        </td>
        <td>xsd:ID</td>
        <td>unique name for this bridge</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html" title="Chapter 36. Core Bridges">bridges.bridge.queue-name</a>
        </td>
        <td>xsd:IDREF</td>
        <td>name of queue that this bridge consumes from</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html"
               title="Chapter 36. Core Bridges">bridges.bridge.forwarding-address</a>
        </td>
        <td>xsd:string</td>
        <td>address to forward to. If omitted original address is used</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html" title="Chapter 36. Core Bridges">bridges.bridge.ha</a>
        </td>
        <td>xsd:boolean</td>
        <td>whether this bridge supports fail-over</td>
        <td>false</td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html" title="Chapter 36. Core Bridges">bridges.bridge.filter</a>
        </td>
        <td>Complex element</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html" title="Chapter 36. Core Bridges">bridges.bridge.filter.string
                (required attribute)</a>
        </td>
        <td>xsd:string</td>
        <td>optional core filter expression</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html" title="Chapter 36. Core Bridges">bridges.bridge.transformer-class-name</a>
        </td>
        <td>xsd:string</td>
        <td>optional name of transformer class</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html" title="Chapter 36. Core Bridges">bridges.bridge.min-large-message-size</a>
        </td>
        <td>xsd:int</td>
        <td>Any message larger than this size is considered a large message (to be sent in chunks)</td>
        <td>102400 (bytes)</td>
    </tr>
    <tr>
        <td>
            <a href="connection-ttl.html" title="Chapter 17. Detecting Dead Connections">bridges.bridge.check-period</a>
        </td>
        <td>xsd:long</td>
        <td>The period (in milliseconds) a bridge's client will check if it failed to receive a ping from the server. -1
            disables this check.
        </td>
        <td>30000 (ms)</td>
    </tr>
    <tr>
        <td>
            <a href="connection-ttl.html" title="Chapter 17. Detecting Dead Connections">bridges.bridge.connection-ttl</a>
        </td>
        <td>xsd:long</td>
        <td>how long to keep a connection alive in the absence of any data arriving from the client. This should be
            greater than the ping period.
        </td>
        <td>60000 (ms)</td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html" title="Chapter 36. Core Bridges">bridges.bridge.retry-interval</a>
        </td>
        <td>xsd:long</td>
        <td>period (in ms) between successive retries</td>
        <td>2000 (in milliseconds)</td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html" title="Chapter 36. Core Bridges">bridges.bridge.retry-interval-multiplier</a>
        </td>
        <td>xsd:double</td>
        <td>multiplier to apply to successive retry intervals</td>
        <td>1</td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html"
               title="Chapter 36. Core Bridges">bridges.bridge.max-retry-interval</a>
        </td>
        <td>xsd:long</td>
        <td>Limit to the retry-interval growth (due to retry-interval-multiplier)</td>
        <td>2000</td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html"
               title="Chapter 36. Core Bridges">bridges.bridge.reconnect-attempts</a>
        </td>
        <td>xsd:int</td>
        <td>maximum number of retry attempts, -1 means 'no limits'</td>
        <td>-1</td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html" title="Chapter 36. Core Bridges">bridges.bridge.use-duplicate-detection</a>
        </td>
        <td>xsd:boolean</td>
        <td>should duplicate detection headers be inserted in forwarded messages?</td>
        <td>true</td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html" title="Chapter 36. Core Bridges">bridges.bridge.confirmation-window-size</a>
        </td>
        <td>xsd:int</td>
        <td>Once the bridge has received this many bytes, it sends a confirmation</td>
        <td>(bytes, 1024 * 1024)</td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html" title="Chapter 36. Core Bridges">bridges.bridge.user</a>
        </td>
        <td>xsd:string</td>
        <td>username, if unspecified the cluster-user is used</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html" title="Chapter 36. Core Bridges">bridges.bridge.password</a>
        </td>
        <td>xsd:string</td>
        <td>password, if unspecified the cluster-password is used</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="core-bridges.html" title="Chapter 36. Core Bridges">bridges.bridge.reconnect-attempts-same-node</a>
        </td>
        <td>xsd:int</td>
        <td>Upon reconnection this configures the number of time the same node on the topology will be retried before
            reseting the server locator and using the initial connectors
        </td>
        <td>10 (int, 10)</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">broadcast-groups</a>
        </td>
        <td>Sequence of &lt;broadcast-group/&gt;</td>
        <td>a list of broadcast groups to create</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">broadcast-groups.broadcast-group</a>
        </td>
        <td>Complex element</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">broadcast-groups.broadcast-group.name
                (required attribute)</a>
        </td>
        <td>xsd:ID</td>
        <td>a unique name for the broadcast group</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">broadcast-groups.broadcast-group.local-bind-address</a>
        </td>
        <td>xsd:string</td>
        <td>local bind address that the datagram socket is bound to</td>
        <td>wildcard IP address chosen by the kernel</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">broadcast-groups.broadcast-group.local-bind-port</a>
        </td>
        <td>xsd:int</td>
        <td>local port to which the datagram socket is bound to</td>
        <td>-1 (anonymous port)</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">broadcast-groups.broadcast-group.group-address</a>
        </td>
        <td>xsd:string</td>
        <td>multicast address to which the data will be broadcast</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">broadcast-groups.broadcast-group.group-port</a>
        </td>
        <td>xsd:int</td>
        <td>UDP port number used for broadcasting</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">broadcast-groups.broadcast-group.broadcast-period</a>
        </td>
        <td>xsd:long</td>
        <td>period in milliseconds between consecutive broadcasts</td>
        <td>2000 (in milliseconds)</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html">broadcast-groups.broadcast-group.jgroups-file</a>
        </td>
        <td>xsd:string</td>
        <td>Name of JGroups configuration file. If specified, the server uses JGroups for broadcasting.</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html">broadcast-groups.broadcast-group.jgroups-channel</a>
        </td>
        <td>xsd:string</td>
        <td>Name of JGroups Channel. If specified, the server uses the named channel for broadcasting.</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">broadcast-groups.broadcast-group.connector-ref</a>
        </td>
        <td>xsd:string</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="ha.html">check-for-live-server</a>
        </td>
        <td>xsd:boolean</td>
        <td>Whether to check the cluster for a (live) server using our own server ID when starting up. This option is
            only necessary for performing 'fail-back' on replicating servers. Strictly speaking this setting only
            applies to live servers and not to backups.
        </td>
        <td>false</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-connections</a>
        </td>
        <td>Sequence of &lt;cluster-connection/&gt;</td>
        <td>a list of cluster connections</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-connections.cluster-connection</a>
        </td>
        <td>Complex element</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-connections.cluster-connection.name
                (required attribute)</a>
        </td>
        <td>xsd:ID</td>
        <td>unique name for this cluster connection</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-connections.cluster-connection.address</a>
        </td>
        <td>xsd:string</td>
        <td>name of the address this cluster connection applies to</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-connections.cluster-connection.connector-ref</a>
        </td>
        <td>xsd:string</td>
        <td>Name of the connector reference to use.</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="connection-ttl.html" title="Chapter 17. Detecting Dead Connections">cluster-connections.cluster-connection.check-period</a>
        </td>
        <td>xsd:long</td>
        <td>The period (in milliseconds) used to check if the cluster connection has failed to receive pings from
            another server
        </td>
        <td>30000 (ms)</td>
    </tr>
    <tr>
        <td>
            <a href="connection-ttl.html" title="Chapter 17. Detecting Dead Connections">cluster-connections.cluster-connection.connection-ttl</a>
        </td>
        <td>xsd:long</td>
        <td>how long to keep a connection alive in the absence of any data arriving from the client</td>
        <td>60000 (ms)</td>
    </tr>
    <tr>
        <td>
            <a href="large-messages.html" title="Chapter 23. Large Messages">cluster-connections.cluster-connection.min-large-message-size</a>
        </td>
        <td>xsd:int</td>
        <td>Messages larger than this are considered large-messages</td>
        <td>(bytes)</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-connections.cluster-connection.call-timeout</a>
        </td>
        <td>xsd:long</td>
        <td>How long to wait for a reply</td>
        <td>30000 (ms)</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-connections.cluster-connection.retry-interval</a>
        </td>
        <td>xsd:long</td>
        <td>period (in ms) between successive retries</td>
        <td>500</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-connections.cluster-connection.retry-interval-multiplier</a>
        </td>
        <td>xsd:double</td>
        <td>multiplier to apply to the retry-interval</td>
        <td>1</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-connections.cluster-connection.max-retry-interval</a>
        </td>
        <td>xsd:long</td>
        <td>Maximum value for retry-interval</td>
        <td>2000</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-connections.cluster-connection.reconnect-attempts</a>
        </td>
        <td>xsd:int</td>
        <td>How many attempts should be made to reconnect after failure</td>
        <td>-1</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-connections.cluster-connection.use-duplicate-detection</a>
        </td>
        <td>xsd:boolean</td>
        <td>should duplicate detection headers be inserted in forwarded messages?</td>
        <td>true</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-connections.cluster-connection.forward-when-no-consumers</a>
        </td>
        <td>xsd:boolean</td>
        <td>should messages be load balanced if there are no matching consumers on target?</td>
        <td>false</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-connections.cluster-connection.max-hops</a>
        </td>
        <td>xsd:int</td>
        <td>maximum number of hops cluster topology is propagated</td>
        <td>1</td>
    </tr>
    <tr>
        <td>
            <a href="client-reconnection.html"
               title="Chapter 34. Client Reconnection and Session Reattachment">cluster-connections.cluster-connection.confirmation-window-size</a>
        </td>
        <td>xsd:int</td>
        <td>The size (in bytes) of the window used for confirming data from the server connected to.</td>
        <td>1048576</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html"
               title="38.3.1. Configuring Cluster Connections">cluster-connections.cluster-connection.call-failover-timeout</a>
        </td>
        <td>xsd:long</td>
        <td>How long to wait for a reply if in the middle of a fail-over. -1 means wait forever.</td>
        <td>-1 (ms)</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-connections.cluster-connection.notification-interval</a>
        </td>
        <td>xsd:long</td>
        <td>how often the cluster connection will notify the cluster of its existence right after joining the cluster
        </td>
        <td>1000 (ms)</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-connections.cluster-connection.notification-attempts</a>
        </td>
        <td>xsd:int</td>
        <td>how many times this cluster connection will notify the cluster of its existence right after joining the
            cluster
        </td>
        <td>2</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">clustered</a>
        </td>
        <td>xsd:boolean</td>
        <td>DEPRECATED. This option is deprecated and its value will be ignored (HQ221038). A HornetQ server will be
            "clustered" when its configuration contain a cluster-configuration.
        </td>
        <td>false</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-password</a>
        </td>
        <td>xsd:string</td>
        <td>Cluster password. It applies to all cluster configurations.</td>
        <td>CHANGE ME!!</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">cluster-user</a>
        </td>
        <td>xsd:string</td>
        <td>Cluster username. It applies to all cluster configurations.</td>
        <td>HORNETQ.CLUSTER.ADMIN.USER</td>
    </tr>
    <tr>
        <td>
            <a href="connection-ttl.html">connection-ttl-override</a>
        </td>
        <td>xsd:long</td>
        <td>if set, this will override how long (in ms) to keep a connection alive without receiving a ping. -1 disables
            this setting.
        </td>
        <td>-1</td>
    </tr>
    <tr>
        <td>
            <a href="configuring-transports.html"
               title="16.2. Understanding Connectors">connectors</a>
        </td>
        <td>Sequence of &lt;connector/&gt;</td>
        <td>a list of remoting connectors configurations to create</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="configuring-transports.html"
               title="16.2. Understanding Connectors">connectors.connector</a>
        </td>
        <td>Complex element</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="configuring-transports.html"
               title="16.2. Understanding Connectors">connectors.connector.name (required attribute)</a>
        </td>
        <td>xsd:ID</td>
        <td>Name of the connector</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="configuring-transports.html"
               title="16.2. Understanding Connectors">connectors.connector.factory-class</a>
        </td>
        <td>xsd:string</td>
        <td>Name of the ConnectorFactory implementation</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="configuring-transports.html"
               title="16.2. Understanding Connectors">connectors.connector.param</a>
        </td>
        <td>Complex element</td>
        <td>A key-value pair used to configure the connector. A connector can have many param's</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="configuring-transports.html"
               title="16.2. Understanding Connectors">connectors.connector.param.key (required attribute)</a>
        </td>
        <td>xsd:string</td>
        <td>Key of a configuration parameter</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="configuring-transports.html"
               title="16.2. Understanding Connectors">connectors.connector.param.value (required attribute)</a>
        </td>
        <td>xsd:string</td>
        <td>Value of a configuration parameter</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="">connector-services</a>
        </td>
        <td>Sequence of &lt;connector-service/&gt;</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="">connector-services.connector-service</a>
        </td>
        <td>Complex element</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="">connector-services.connector-service.name (attribute)</a>
        </td>
        <td>xsd:string</td>
        <td>name of the connector service</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="">connector-services.connector-service.factory-class</a>
        </td>
        <td>xsd:string</td>
        <td>Name of the factory class of the ConnectorService</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="">connector-services.connector-service.param</a>
        </td>
        <td>Complex element</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="">connector-services.connector-service.param.key (required attribute)</a>
        </td>
        <td>xsd:string</td>
        <td>Key of a configuration parameter</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="">connector-services.connector-service.param.value (required attribute)</a>
        </td>
        <td>xsd:string</td>
        <td>Value of a configuration parameter</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="persistence.html"
               title="15.1. Configuring the bindings journal">create-bindings-dir</a>
        </td>
        <td>xsd:boolean</td>
        <td>true means that the server will create the bindings directory on start up</td>
        <td>true</td>
    </tr>
    <tr>
        <td>
            <a class="link"
               href="persistence.html">create-journal-dir</a>
        </td>
        <td>xsd:boolean</td>
        <td>true means that the journal directory will be created</td>
        <td>true</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">discovery-groups</a>
        </td>
        <td>Sequence of &lt;discovery-group/&gt;</td>
        <td>a list of discovery groups to create</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">discovery-groups.discovery-group</a>
        </td>
        <td>Complex element</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">discovery-groups.discovery-group.name
                (required attribute)</a>
        </td>
        <td>xsd:ID</td>
        <td>a unique name for the discovery group</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">discovery-groups.discovery-group.group-address</a>
        </td>
        <td>xsd:string</td>
        <td>Multicast IP address of the group to listen on</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">discovery-groups.discovery-group.group-port</a>
        </td>
        <td>xsd:int</td>
        <td>UDP port number of the multi cast group</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html">discovery-groups.discovery-group.jgroups-file</a>
        </td>
        <td>xsd:string</td>
        <td>Name of a JGroups configuration file. If specified, the server uses JGroups for discovery.</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html">discovery-groups.discovery-group.jgroups-channel</a>
        </td>
        <td>xsd:string</td>
        <td>Name of a JGroups Channel. If specified, the server uses the named channel for discovery.</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="">discovery-groups.discovery-group.refresh-timeout</a>
        </td>
        <td>xsd:int</td>
        <td>Period the discovery group waits after receiving the last broadcast from a particular server before removing
            that servers connector pair entry from its list.
        </td>
        <td>10000 (in milliseconds)</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">discovery-groups.discovery-group.local-bind-address</a>
        </td>
        <td>xsd:string</td>
        <td>local bind address that the datagram socket is bound to</td>
        <td>wildcard IP address chosen by the kernel</td>
    </tr>
    <tr>
        <td>
            <a href="clusters.html" title="Chapter 38. Clusters">discovery-groups.discovery-group.local-bind-port</a>
        </td>
        <td>xsd:int</td>
        <td>local port to which the datagram socket is bound to</td>
        <td>-1 (anonymous port)</td>
    </tr>
    <tr>
        <td>
            <a href="">discovery-groups.discovery-group.initial-wait-timeout</a>
        </td>
        <td>xsd:int</td>
        <td>time to wait for an initial broadcast to give us at least one node in the cluster</td>
        <td>10000 (milliseconds)</td>
    </tr>
    <tr>
        <td>
            <a href="diverts.html" title="Chapter 35. Diverting and Splitting Message Flows">diverts</a>
        </td>
        <td>Sequence of &lt;divert/&gt;</td>
        <td>a list of diverts to use</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="diverts.html"
               title="Chapter 35. Diverting and Splitting Message Flows">diverts.divert</a>
        </td>
        <td>Complex element</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="diverts.html" title="Chapter 35. Diverting and Splitting Message Flows">diverts.divert.name
                (required attribute)</a>
        </td>
        <td>xsd:ID</td>
        <td>a unique name for the divert</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="diverts.html" title="Chapter 35. Diverting and Splitting Message Flows">diverts.divert.transformer-class-name</a>
        </td>
        <td>xsd:string</td>
        <td>an optional class name of a transformer</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="diverts.html" title="Chapter 35. Diverting and Splitting Message Flows">diverts.divert.exclusive</a>
        </td>
        <td>xsd:boolean</td>
        <td>whether this is an exclusive divert</td>
        <td>false</td>
    </tr>
    <tr>
        <td>
            <a href="diverts.html" title="Chapter 35. Diverting and Splitting Message Flows">diverts.divert.routing-name</a>
        </td>
        <td>xsd:string</td>
        <td>the routing name for the divert</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="diverts.html" title="Chapter 35. Diverting and Splitting Message Flows">diverts.divert.address</a>
        </td>
        <td>xsd:string</td>
        <td>the address this divert will divert from</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="diverts.html" title="Chapter 35. Diverting and Splitting Message Flows">diverts.divert.forwarding-address</a>
        </td>
        <td>xsd:string</td>
        <td>the forwarding address for the divert</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="diverts.html" title="Chapter 35. Diverting and Splitting Message Flows">diverts.divert.filter</a>
        </td>
        <td>Complex element</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="diverts.html" title="Chapter 35. Diverting and Splitting Message Flows">diverts.divert.filter.string
                (required attribute)</a>
        </td>
        <td>xsd:string</td>
        <td>optional core filter expression</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="graceful-shutdown.html" title="Graceful Server Shutdown">graceful-shutdown-enabled</a>
        </td>
        <td>xsd:boolean</td>
        <td>true means that graceful shutdown is enabled</td>
        <td>true</td>
    </tr>
    <tr>
        <td>
            <a href="graceful-shutdown.html" title="Graceful Server Shutdown">graceful-shutdown-timeout</a>
        </td>
        <td>xsd:long</td>
        <td>how long (in ms) to wait for all clients to disconnect before forcefully disconnecting the clients and proceeding with the shutdown process (-1 means no timeout)</td>
        <td>-1</td>
    </tr>
    <tr>
        <td>
            <a href="message-grouping.html" title="Chapter 28. Message Grouping">grouping-handler</a>
        </td>
        <td>Complex element</td>
        <td>Message Group configuration</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="message-grouping.html" title="Chapter 28. Message Grouping">grouping-handler.name
                (required attribute)</a>
        </td>
        <td>xsd:string</td>
        <td>A name identifying this grouping-handler</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="message-grouping.html">grouping-handler.type</a>
        </td>
        <td>LOCAL|REMOTE</td>
        <td>Each cluster should choose 1 node to have a LOCAL grouping handler and all the other nodes should have
            REMOTE handlers
        </td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="message-grouping.html">grouping-handler.address</a>
        </td>
        <td>xsd:string</td>
        <td>A reference to a cluster connection address</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="message-grouping.html"
               title="Chapter 28. Message Grouping">grouping-handler.timeout</a>
        </td>
        <td>xsd:int</td>
        <td>How long to wait for a decision</td>
        <td>5000 (ms)</td>
    </tr>
    <tr>
        <td>
            <a href="message-grouping.html" title="Chapter 28. Message Grouping">grouping-handler.group-timeout</a>
        </td>
        <td>xsd:int</td>
        <td>How long a group binding will be used, -1 means for ever. Bindings are removed after this wait elapses. Only
            valid for LOCAL handlers
        </td>
        <td>-1 (ms)</td>
    </tr>
    <tr>
        <td>
            <a href="message-grouping.html" title="Chapter 28. Message Grouping">grouping-handler.reaper-period</a>
        </td>
        <td>xsd:long</td>
        <td>How often the reaper will be run to check for timed out group bindings. Only valid for LOCAL handlers</td>
        <td>30000 (ms)</td>
    </tr>
    <tr>
        <td>
            <a href="duplicate-detection.html"
               title="37.2. Configuring the Duplicate ID Cache">id-cache-size</a>
        </td>
        <td>xsd:int</td>
        <td>the size of the cache for pre-creating message ID's</td>
        <td>20000</td>
    </tr>
    <tr>
        <td>
            <a href="management.html" title="30.2.1. Configuring JMX">jmx-domain</a>
        </td>
        <td>xsd:string</td>
        <td>the JMX domain used to registered HornetQ MBeans in the MBeanServer</td>
        <td>org.hornetq</td>
    </tr>
    <tr>
        <td>
            <a href="management.html" title="30.2.1. Configuring JMX">jmx-management-enabled</a>
        </td>
        <td>xsd:boolean</td>
        <td>true means that the management API is available via JMX</td>
        <td>true</td>
    </tr>
    <tr>
        <td>
            <a class="link"
               href="persistence.html">journal-buffer-size</a>
        </td>
        <td>xsd:long</td>
        <td>The size of the internal buffer on the journal in KiB.</td>
        <td>501760 (490 KiB)</td>
    </tr>
    <tr>
        <td>
            <a href="persistence.html">journal-buffer-timeout</a>
        </td>
        <td>xsd:long</td>
        <td>The timeout (in nanoseconds) used to flush internal buffers on the journal. The exact default value depend
            on whether the journal is ASYNCIO or NIO.
        </td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="persistence.html">journal-compact-min-files</a>
        </td>
        <td>xsd:int</td>
        <td>The minimal number of data files before we can start compacting</td>
        <td>10</td>
    </tr>
    <tr>
        <td>
            <a href="persistence.html">journal-compact-percentage</a>
        </td>
        <td>xsd:int</td>
        <td>The percentage of live data on which we consider compacting the journal</td>
        <td>30</td>
    </tr>
    <tr>
        <td>
            <a href="persistence.html">journal-directory</a>
        </td>
        <td>xsd:string</td>
        <td>the directory to store the journal files in</td>
        <td>data/journal</td>
    </tr>
    <tr>
        <td>
            <a href="persistence.html">journal-file-size</a>
        </td>
        <td>xsd:int</td>
        <td>the size (in bytes) of each journal file</td>
        <td>10485760 (10 * 1024 * 1024 - 10 MiB)</td>
    </tr>
    <tr>
        <td>
            <a href="persistence.html#configuring.message.journal.journal-max-io">journal-max-io</a>
        </td>
        <td>xsd:int</td>
        <td>the maximum number of write requests that can be in the AIO queue at any one time. Default is 500 for AIO
            and 1 for NIO.
        </td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="persistence.html#configuring.message.journal.journal-min-files">journal-min-files</a>
        </td>
        <td>xsd:int</td>
        <td>how many journal files to pre-create</td>
        <td>2</td>
    </tr>
    <tr>
        <td>
            <a href="persistence.html">journal-sync-non-transactional</a>
        </td>
        <td>xsd:boolean</td>
        <td>if true wait for non transaction data to be synced to the journal before returning response to client.</td>
        <td>true</td>
    </tr>
    <tr>
        <td>
            <a href="persistence.html">journal-sync-transactional</a>
        </td>
        <td>xsd:boolean</td>
        <td>if true wait for transaction data to be synchronized to the journal before returning response to client</td>
        <td>true</td>
    </tr>
    <tr>
        <td>
            <a href="persistence.html">journal-type</a>
        </td>
        <td>ASYNCIO|NIO</td>
        <td>the type of journal to use</td>
        <td>ASYNCIO</td>
    </tr>
    <tr>
        <td>
            <a href="large-messages.html" title="23.1. Configuring the server">large-messages-directory</a>
        </td>
        <td>xsd:string</td>
        <td>the directory to store large messages</td>
        <td>data/largemessages</td>
    </tr>
    <tr>
        <td>
            <a href="">log-delegate-factory-class-name</a>
        </td>
        <td>xsd:string</td>
        <td>XXX</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="">log-journal-write-rate</a>
        </td>
        <td>xsd:boolean</td>
        <td>Whether to log messages about the journal write rate</td>
        <td>false</td>
    </tr>
    <tr>
        <td>
            <a href="management.html"
               title="30.3.1. Configuring Core Management">management-address</a>
        </td>
        <td>xsd:string</td>
        <td>the name of the management address to send management messages to. It is prefixed with "jms.queue" so that
            JMS clients can send messages to it.
        </td>
        <td>jms.queue.hornetq.management</td>
    </tr>
    <tr>
        <td>
            <a href="management.html"
               title="30.5.2.1. Configuring The Core Management Notification Address">management-notification-address</a>
        </td>
        <td>xsd:string</td>
        <td>the name of the address that consumers bind to receive management notifications</td>
        <td>hornetq.notifications</td>
    </tr>
    <tr>
        <td>
            <a href="configuration-index.html"
               title="50.1.3. Using Masked Passwords in Configuration Files">mask-password</a>
        </td>
        <td>xsd:boolean</td>
        <td>This option controls whether passwords in server configuration need be masked. If set to "true" the
            passwords are masked.
        </td>
        <td>false</td>
    </tr>
    <tr>
        <td>
            <a href="">max-saved-replicated-journals-size</a>
        </td>
        <td>xsd:int</td>
        <td>This specifies how many times a replicated backup server can restart after moving its files on start. Once
            there are this number of backup journal files the server will stop permanently after if fails back.
        </td>
        <td>2</td>
    </tr>
    <tr>
        <td>
            <a href="perf-tuning.html">memory-measure-interval</a>
        </td>
        <td>xsd:long</td>
        <td>frequency to sample JVM memory in ms (or -1 to disable memory sampling)</td>
        <td>-1 (ms)</td>
    </tr>
    <tr>
        <td>
            <a href="perf-tuning.html">memory-warning-threshold</a>
        </td>
        <td>xsd:int</td>
        <td>Percentage of available memory which will trigger a warning log</td>
        <td>25</td>
    </tr>
    <tr>
        <td>
            <a href="management.html"
               title="30.6.1. Configuring Message Counters">message-counter-enabled</a>
        </td>
        <td>xsd:boolean</td>
        <td>true means that message counters are enabled</td>
        <td>false</td>
    </tr>
    <tr>
        <td>
            <a href="management.html"
               title="30.6.1. Configuring Message Counters">message-counter-max-day-history</a>
        </td>
        <td>xsd:int</td>
        <td>how many days to keep message counter history</td>
        <td>10 (days)</td>
    </tr>
    <tr>
        <td>
            <a href="management.html"
               title="30.6.1. Configuring Message Counters">message-counter-sample-period</a>
        </td>
        <td>xsd:long</td>
        <td>the sample period (in ms) to use for message counters</td>
        <td>10000</td>
    </tr>
    <tr>
        <td>
            <a href="message-expiry.html"
               title="22.3. Configuring The Expiry Reaper Thread">message-expiry-scan-period</a>
        </td>
        <td>xsd:long</td>
        <td>how often (in ms) to scan for expired messages</td>
        <td>30000</td>
    </tr>
    <tr>
        <td>
            <a href="message-expiry.html"
               title="22.3. Configuring The Expiry Reaper Thread">message-expiry-thread-priority</a>
        </td>
        <td>xsd:int</td>
        <td>the priority of the thread expiring messages</td>
        <td>3</td>
    </tr>
    <tr>
        <td>
            <a href="">name</a>
        </td>
        <td>xsd:string</td>
        <td>Node name. If set, it will be used in topology notifications.</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="paging.html" title="24.3. Paging Mode">page-max-concurrent-io</a>
        </td>
        <td>xsd:int</td>
        <td>The max number of concurrent reads allowed on paging</td>
        <td>5</td>
    </tr>
    <tr>
        <td>
            <a href="paging.html" title="24.2. Configuration">paging-directory</a>
        </td>
        <td>xsd:string</td>
        <td>the directory to store paged messages in</td>
        <td>data/paging</td>
    </tr>
    <tr>
        <td>
            <a href="configuration-index.html"
               title="50.1.3. Using Masked Passwords in Configuration Files">password-codec</a>
        </td>
        <td>xsd:string</td>
        <td>Class name and its parameters for the Decoder used to decode the masked password. Ignored if mask-password
            is false. The format of this property is a full qualified class name optionally followed by key/value pairs.
        </td>
        <td>org.hornetq.utils.DefaultSensitiveStringCodec</td>
    </tr>
    <tr>
        <td>
            <a href="">perf-blast-pages</a>
        </td>
        <td>xsd:int</td>
        <td>XXX Only meant to be used by project developers</td>
        <td>-1</td>
    </tr>
    <tr>
        <td>
            <a href="undelivered-messages.html"
               title="21.3. Delivery Count Persistence">persist-delivery-count-before-delivery</a>
        </td>
        <td>xsd:boolean</td>
        <td>True means that the delivery count is persisted before delivery. False means that this only happens after a
            message has been cancelled.
        </td>
        <td>false</td>
    </tr>
    <tr>
        <td>
            <a href="persistence.html"
               title="15.6. Configuring HornetQ for Zero Persistence">persistence-enabled</a>
        </td>
        <td>xsd:boolean</td>
        <td>true means that the server will use the file based journal for persistence.</td>
        <td>true</td>
    </tr>
    <tr>
        <td>
            <a href="duplicate-detection.html"
               title="37.2. Configuring the Duplicate ID Cache">persist-id-cache</a>
        </td>
        <td>xsd:boolean</td>
        <td>true means that ID's are persisted to the journal</td>
        <td>true</td>
    </tr>
    <tr>
        <td>
            <a href="queue-attributes.html" title="25.1. Predefined Queues">queues</a>
        </td>
        <td>Sequence of &lt;queue/&gt;</td>
        <td>a list of pre configured queues to create</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="queue-attributes.html"
               title="25.1. Predefined Queues">queues.queue</a>
        </td>
        <td>Complex element</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="queue-attributes.html" title="25.1. Predefined Queues">queues.queue.name
                (required attribute)</a>
        </td>
        <td>xsd:ID</td>
        <td>unique name of this queue</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="queue-attributes.html" title="25.1. Predefined Queues">queues.queue.address</a>
        </td>
        <td>xsd:string</td>
        <td>address for the queue</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="queue-attributes.html" title="25.1. Predefined Queues">queues.queue.filter</a>
        </td>
        <td>Complex element</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="queue-attributes.html" title="25.1. Predefined Queues">queues.queue.filter.string
                (required attribute)</a>
        </td>
        <td>xsd:string</td>
        <td>optional core filter expression</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="queue-attributes.html" title="25.1. Predefined Queues">queues.queue.durable</a>
        </td>
        <td>xsd:boolean</td>
        <td>whether the queue is durable (persistent)</td>
        <td>true</td>
    </tr>
    <tr>
        <td>
            <a href="intercepting-operations.html" title="Chapter 47. Intercepting Operations">remoting-incoming-interceptors</a>
        </td>
        <td>Complex element</td>
        <td>a list of &lt;class-name/&gt; elements with the names of classes to use for interceptor incoming remoting
            packetsunlimited sequence of &lt;class-name/&gt;</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="intercepting-operations.html" title="Chapter 47. Intercepting Operations">remoting-incoming-interceptors.class-name</a>
        </td>
        <td>xsd:string</td>
        <td>the fully qualified name of the interceptor class</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="intercepting-operations.html" title="Chapter 47. Intercepting Operations">remoting-interceptors</a>
        </td>
        <td>Complex element</td>
        <td>DEPRECATED. This option is deprecated, but it will still be honored. Any interceptor specified here will be
            considered an "incoming" interceptor. See &lt;remoting-incoming-interceptors&gt; and &lt;remoting-outgoing-interceptors&gt;.unlimited
            sequence of &lt;class-name/&gt;</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="intercepting-operations.html" title="Chapter 47. Intercepting Operations">remoting-interceptors.class-name</a>
        </td>
        <td>xsd:string</td>
        <td>the fully qualified name of the interceptor class</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="intercepting-operations.html" title="Chapter 47. Intercepting Operations">remoting-outgoing-interceptors</a>
        </td>
        <td>Complex element</td>
        <td>a list of &lt;class-name/&gt; elements with the names of classes to use for interceptor outcoming remoting
            packetsunlimited sequence of &lt;class-name/&gt;</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="intercepting-operations.html" title="Chapter 47. Intercepting Operations">remoting-outgoing-interceptors.class-name</a>
        </td>
        <td>xsd:string</td>
        <td>the fully qualified name of the interceptor class</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="">resolveProtocols</a>
        </td>
        <td>xsd:boolean</td>
        <td>If true then the HornetQ Server will make use of any Protocol Managers that are in available on the
            classpath. If false then only the core protocol will be available, unless in Embedded mode where users can
            inject their own Protocol Managers.
        </td>
        <td>true</td>
    </tr>
    <tr>
        <td>
            <a href="">run-sync-speed-test</a>
        </td>
        <td>xsd:boolean</td>
        <td>XXX Only meant to be used by project developers</td>
        <td>false</td>
    </tr>
    <tr>
        <td>
            <a href="thread-pooling.html#server.scheduled.thread.pool"
               title="41.1.1. Server Scheduled Thread Pool">scheduled-thread-pool-max-size</a>
        </td>
        <td>xsd:int</td>
        <td>Maximum number of threads to use for the scheduled thread pool</td>
        <td>5</td>
    </tr>
    <tr>
        <td>
            <a href="security.html" title="Chapter 31. Security">security-enabled</a>
        </td>
        <td>xsd:boolean</td>
        <td>true means that security is enabled</td>
        <td>true</td>
    </tr>
    <tr>
        <td>
            <a href="security.html" title="Chapter 31. Security">security-invalidation-interval</a>
        </td>
        <td>xsd:long</td>
        <td>how long (in ms) to wait before invalidating the security cache</td>
        <td>10000</td>
    </tr>
    <tr>
        <td>
            <a href="security.html"
               title="31.1. Role based security for addresses">security-settings</a>
        </td>
        <td>Sequence of &lt;security-setting/&gt;</td>
        <td>a list of security settings</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="security.html"
               title="31.1. Role based security for addresses">security-settings.security-setting</a>
        </td>
        <td>Sequence of &lt;permission/&gt;</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="security.html"
               title="31.1. Role based security for addresses">security-settings.security-setting.match (required
                attribute)</a>
        </td>
        <td>xsd:string</td>
        <td>regular expression for matching security roles against addresses</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="security.html"
               title="31.1. Role based security for addresses">security-settings.security-setting.permission</a>
        </td>
        <td>Complex element</td>
        <td></td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="security.html"
               title="31.1. Role based security for addresses">security-settings.security-setting.permission.type
                (required attribute)</a>
        </td>
        <td>xsd:string</td>
        <td>the type of permission</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="security.html"
               title="31.1. Role based security for addresses">security-settings.security-setting.permission.roles
                (required attribute)</a>
        </td>
        <td>xsd:string</td>
        <td>a comma-separated list of roles to apply the permission to</td>
        <td></td>
    </tr>
    <tr>
        <td>
            <a href="">server-dump-interval</a>
        </td>
        <td>xsd:long</td>
        <td>Interval to log server specific information (e.g. memory usage etc)</td>
        <td>-1 (ms)</td>
    </tr>
    <tr>
        <td>
            <a href="thread-pooling.html"
               title="41.1.1. Server Scheduled Thread Pool">thread-pool-max-size</a>
        </td>
        <td>xsd:int</td>
        <td>Maximum number of threads to use for the thread pool. -1 means 'no limits'.</td>
        <td>30</td>
    </tr>
    <tr>
        <td>
            <a href="transaction-config.html" title="Chapter 18. Resource Manager Configuration">transaction-timeout</a>
        </td>
        <td>xsd:long</td>
        <td>how long (in ms) before a transaction can be removed from the resource manager after create time</td>
        <td>300000</td>
    </tr>
    <tr>
        <td>
            <a href="transaction-config.html" title="Chapter 18. Resource Manager Configuration">transaction-timeout-scan-period</a>
        </td>
        <td>xsd:long</td>
        <td>how often (in ms) to scan for timeout transactions</td>
        <td>1000</td>
    </tr>
    <tr>
        <td>
            <a href="wildcard-routing.html" title="Chapter 12. Routing Messages With Wild Cards">wild-card-routing-enabled</a>
        </td>
        <td>xsd:boolean</td>
        <td>true means that the server supports wild card routing</td>
        <td>true</td>
    </tr>
    </tbody>
</table>

##The jms configuration


<table summary="JMS Server Configuration" border="1">
    <colgroup>
        <col/>
        <col/>
        <col/>
        <col/>
    </colgroup>
    <thead>
    <tr>
        <th>Element Name</th>
        <th>Element Type</th>
        <th>Description</th>
        <th>Default</th>
    </tr>
    </thead>
    <tr>
        <td><a href="using-jms.html" title="7.2. JMS Server Configuration">queue</a>
        </td>
        <td>Queue</td>
        <td>a queue to create</td>
        <td></td>
    </tr>
    <tr>
        <td><a href="using-jms.html" title="7.2. JMS Server Configuration">queue.name
            (attribute)</a></td>
        <td>String</td>
        <td>unique name of the queue</td>
        <td></td>
    </tr>
    <tr>
        <td><a href="using-jms.html" title="7.2. JMS Server Configuration">queue.durable</a>
        </td>
        <td>Boolean</td>
        <td>is the queue durable?</td>
        <td>true</td>
    </tr>
    <tr>
        <td><a href="using-jms.html" title="7.2. JMS Server Configuration">queue.filter</a>
        </td>
        <td>String</td>
        <td>optional filter expression for the queue</td>
        <td></td>
    </tr>
    <tr>
        <td><a href="using-jms.html" title="7.2. JMS Server Configuration">topic</a>
        </td>
        <td>Topic</td>
        <td>a topic to create</td>
        <td></td>
    </tr>
    <tr>
        <td><a href="using-jms.html" title="7.2. JMS Server Configuration">topic.name
            (attribute)</a></td>
        <td>String</td>
        <td>unique name of the topic</td>
        <td></td>
    </tr>
</table>

Using Masked Passwords in Configuration Files
---------------------------------------------

By default all passwords in Apache ActiveMQ server's configuration files are in
plain text form. This usually poses no security issues as those files
should be well protected from unauthorized accessing. However, in some
circumstances a user doesn't want to expose its passwords to more eyes
than necessary.

Apache ActiveMQ can be configured to use 'masked' passwords in its
configuration files. A masked password is an obscure string
representation of a real password. To mask a password a user will use an
'encoder'. The encoder takes in the real password and outputs the masked
version. A user can then replace the real password in the configuration
files with the new masked password. When Apache ActiveMQ loads a masked
password, it uses a suitable 'decoder' to decode it into real password.

Apache ActiveMQ provides a default password encoder and decoder. Optionally
users can use or implement their own encoder and decoder for masking the
passwords.

### Password Masking in Server Configuration File

#### The password masking property

The server configuration file has a property that defines the default
masking behaviors over the entire file scope.

`mask-password`: this boolean type property indicates if a password
should be masked or not. Set it to "true" if you want your passwords
masked. The default value is "false".

#### Specific masking behaviors

##### cluster-password

The nature of the value of cluster-password is subject to the value of
property 'mask-password'. If it is true the cluster-password is masked.

##### Passwords in connectors and acceptors

In the server configuration, Connectors and Acceptors sometimes needs to
specify passwords. For example if a users wants to use an SSL-enabled
NettyAcceptor, it can specify a key-store-password and a
trust-store-password. Because Acceptors and Connectors are pluggable
implementations, each transport will have different password masking
needs.

When a Connector or Acceptor configuration is initialised, Apache ActiveMQ will
add the "mask-password" and "password-codec" values to the Connector or
Acceptors params using the keys `activemq.usemaskedpassword` and
`activemq.passwordcodec` respectively. The Netty and InVM
implementations will use these as needed and any other implementations
will have access to these to use if they so wish.

##### Passwords in Core Bridge configurations

Core Bridges are configured in the server configuration file and so the
masking of its 'password' properties follows the same rules as that of
'cluster-password'.

#### Examples

The following table summarizes the relations among the above-mentioned
properties

  mask-password   cluster-password   acceptor/connector passwords   bridge password
  --------------- ------------------ ------------------------------ -----------------
  absent          plain text         plain text                     plain text
  false           plain text         plain text                     plain text
  true            masked             masked                         masked

Examples

Note: In the following examples if related attributed or properties are
absent, it means they are not specified in the configure file.

example 1

    <cluster-password>bbc</cluster-password>

This indicates the cluster password is a plain text value ("bbc").

example 2

    <mask-password>true</mask-password>
    <cluster-password>80cf731af62c290</cluster-password>

This indicates the cluster password is a masked value and Apache ActiveMQ will
use its built-in decoder to decode it. All other passwords in the
configuration file, Connectors, Acceptors and Bridges, will also use
masked passwords.

### JMS Bridge password masking

The JMS Bridges are configured and deployed as separate beans so they
need separate configuration to control the password masking. A JMS
Bridge has two password parameters in its constructor, SourcePassword
and TargetPassword. It uses the following two optional properties to
control their masking:

`useMaskedPassword` -- If set to "true" the passwords are masked.
Default is false.

`passwordCodec` -- Class name and its parameters for the Decoder used to
decode the masked password. Ignored if `useMaskedPassword` is false. The
format of this property is a full qualified class name optionally
followed by key/value pairs, separated by semi-colons. For example:

\<property name="useMaskedPassword"\>true\</property\>
\<property
name="passwordCodec"\>com.foo.FooDecoder;key=value\</property\>
Apache ActiveMQ will load this property and initialize the class with a
parameter map containing the "key"-\>"value" pair. If `passwordCodec` is
not specified, the built-in decoder is used.

### Masking passwords in ActiveMQ ResourceAdapters and MDB activation configurations

Both ra.xml and MDB activation configuration have a 'password' property
that can be masked. They are controlled by the following two optional
Resource Adapter properties in ra.xml:

`UseMaskedPassword` -- If setting to "true" the passwords are masked.
Default is false.

`PasswordCodec` -- Class name and its parameters for the Decoder used to
decode the masked password. Ignored if UseMaskedPassword is false. The
format of this property is a full qualified class name optionally
followed by key/value pairs. It is the same format as that for JMS
Bridges. Example:

    <config-property>
      <config-property-name>UseMaskedPassword</config-property-name>
      <config-property-type>boolean</config-property-type>
      <config-property-value>true</config-property-value>
    </config-property>
    <config-property>
      <config-property-name>PasswordCodec</config-property-name>
      <config-property-type>java.lang.String</config-property-type>
      <config-property-value>com.foo.ADecoder;key=helloworld</config-property-value>
    </config-property>

With this configuration, both passwords in ra.xml and all of its MDBs
will have to be in masked form.

### Masking passwords in activemq-users.properties

Apache ActiveMQ's built-in security manager uses plain properties files
where the user passwords are specified in plaintext forms by default. To
mask those parameters the following two properties need to be set
in the 'bootstrap.xml' file.

`mask-password` -- If set to "true" all the passwords are masked.
Default is false.

`password-codec` -- Class name and its parameters for the Decoder used
to decode the masked password. Ignored if `mask-password` is false. The
format of this property is a full qualified class name optionally
followed by key/value pairs. It is the same format as that for JMS
Bridges. Example:

    <mask-password>true</mask-password>
    <password-codec>org.apache.activemq.utils.DefaultSensitiveStringCodec;key=hello world</password-codec>

When so configured, the Apache ActiveMQ security manager will initialize a
DefaultSensitiveStringCodec with the parameters "key"-\>"hello world",
then use it to decode all the masked passwords in this configuration
file.

### Choosing a decoder for password masking

As described in the previous sections, all password masking requires a
decoder. A decoder uses an algorithm to convert a masked password into
its original clear text form in order to be used in various security
operations. The algorithm used for decoding must match that for
encoding. Otherwise the decoding may not be successful.

For user's convenience Apache ActiveMQ provides a default built-in Decoder.
However a user can if they so wish implement their own.

#### The built-in Decoder

Whenever no decoder is specified in the configuration file, the built-in
decoder is used. The class name for the built-in decoder is
org.apache.activemq.utils.DefaultSensitiveStringCodec. It has both
encoding and decoding capabilities. It uses java.crypto.Cipher utilities
to encrypt (encode) a plaintext password and decrypt a mask string using
same algorithm. Using this decoder/encoder is pretty straightforward. To
get a mask for a password, just run the following in command line:

    java org.apache.activemq.utils.DefaultSensitiveStringCodec "your plaintext password"

Make sure the classpath is correct. You'll get something like

    Encoded password: 80cf731af62c290

Just copy "80cf731af62c290" and replace your plaintext password with it.

#### Using a different decoder

It is possible to use a different decoder rather than the built-in one.
Simply make sure the decoder is in Apache ActiveMQ's classpath and configure
the server to use it as follows:

    <password-codec>com.foo.SomeDecoder;key1=value1;key2=value2</password-codec>

If your decoder needs params passed to it you can do this via key/value
pairs when configuring. For instance if your decoder needs say a
"key-location" parameter, you can define like so:

    <password-codec>com.foo.NewDecoder;key-location=/some/url/to/keyfile</password-codec>

Then configure your cluster-password like this:

    <mask-password>true</mask-password>
    <cluster-password>masked_password</cluster-password>

When Apache ActiveMQ reads the cluster-password it will initialize the
NewDecoder and use it to decode "mask\_password". It also process all
passwords using the new defined decoder.

#### Implementing your own codecs

To use a different decoder than the built-in one, you either pick one
from existing libraries or you implement it yourself. All decoders must
implement the `org.apache.activemq.utils.SensitiveDataCodec<T>`
interface:

    public interface SensitiveDataCodec<T>
    {
       T decode(Object mask) throws Exception;

       void init(Map<String, String> params);
    }

This is a generic type interface but normally for a password you just
need String type. So a new decoder would be defined like

    public class MyNewDecoder implements SensitiveDataCodec<String>
    {
       public String decode(Object mask) throws Exception
       {
          //decode the mask into clear text password
          return "the password";
       }

       public void init(Map<String, String> params)
       {
          //initialization done here. It is called right after the decoder has been created.
       }
    }

Last but not least, once you get your own decoder, please add it to the
classpath. Otherwise Apache ActiveMQ will fail to load it!
