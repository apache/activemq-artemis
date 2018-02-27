Configuration Reference
=======================

This section is a quick index for looking up configuration. Click on the
element name to go to the specific chapter.

Server Configuration
====================

broker.xml
--------------------------

This is the main core server configuration file which contains the 'core'
element.
The 'core' element contains the main server configuration.

# System properties

It is possible to use System properties to replace some of the configuration properties. If you define a System property starting with "brokerconfig." that will be passed along to Bean Utils and the configuration would be replaced.

To define global-max-size=1000000 using a system property you would have to define this property, for example through java arguments:

```
java -Dbrokerconfig.globalMaxSize=1000000
```

You can also change the prefix through the broker.xml by setting:

```
<system-property-prefix>yourprefix</system-property-prefix>
```

This is to help you customize artemis on embedded systems.

# Modularising config into separate files.

XML XInclude support is provided in the configuration as such if you wish to break your configuration out into separate files you can.

To do this ensure the following is defined at the root configuration element.

```
   xmlns:xi="http://www.w3.org/2001/XInclude"
```

You can now define include tag's where you want to bring in xml configuration from another file:

```
   <xi:include href="my-address-settings.xml"/>
```

You should ensure xml elements in separated files should be namespaced correctly for example if address-settings element was separated, it should have the element namespace defined:

```
 <address-settings xmlns="urn:activemq:core">
```

An example can of this feature can be seen in the test suites:
``` 
   ./artemis-server/src/test/resources/ConfigurationTest-xinclude-config.xml
```
N.B. if you use xmllint to validate xml's against schema you should enable xinclude flag when running.

```
   --xinclude
```

For further information on XInclude see:

[https://www.w3.org/TR/xinclude/](https://www.w3.org/TR/xinclude/) 


# The core configuration

This describes the root of the XML configuration. You will see here also multiple sub-types listed.
For example on the main config you will have bridges and at the [list of bridge](#bridge-type) type we will describe the properties for that configuration.

Name | Description
:--- | :---
[acceptors](configuring-transports.md "Understanding Acceptors") | a list of remoting acceptors
[acceptors.acceptor](configuring-transports.md "Understanding Acceptors") | Each acceptor is composed for just an URL
[address-settings](address-model.md "Configuring Addresses and Queues Via Address Settings")                                                    |  [a list of address-setting](#address-setting-type)
[allow-failback](ha.md "Failing Back to live Server")                                                                              |  Should stop backup on live restart. default true
[amqp-use-core-subscription-naming](using-AMQP.md "Message Conversions")  | If true uses CORE queue naming convention for AMQP. default false
[async-connection-execution-enabled](connection-ttl.md "Configuring Asynchronous Connection Execution")  | If False delivery would be always asynchronous. default true
[bindings-directory](persistence.md "Configuring the bindings journal")  | The folder in use for the bindings folder
[bridges](core-bridges.md "Core Bridges")  | [a list of bridge](#bridge-type)
[broadcast-groups](clusters.md "Clusters")                                            | [a list of broadcast-group](#broadcast-group-type)
[configuration-file-refresh-period](config-reload.md) |  The frequency in milliseconds the configuration file is checked for changes (default 5000)
[check-for-live-server](ha.md)   |  Used for a live server to verify if there are other nodes with the same ID on the topology
[cluster-connections](clusters.md "Clusters") |  [a list of cluster-connection](#cluster-connection-type)
[cluster-password](clusters.md "Clusters")                                                                                              |   Cluster password. It applies to all cluster configurations.
[cluster-user](clusters.md "Clusters")                                                                                                  |   Cluster username. It applies to all cluster configurations.
[connection-ttl-override](connection-ttl.md)                                                                                                        |   if set, this will override how long (in ms) to keep a connection alive without receiving a ping. -1 disables this setting. Default -1
[connection-ttl-check-period](connection-ttl.md)                                                                                                    |   how often (in ms) to check connections for ttl violation. Default 2000
[connectors.connector](configuring-transports.md "Understanding Connectors") | The URL for the connector. This is a list
[create-bindings-dir](persistence.md "Configuring the bindings journal") |  true means that the server will create the bindings directory on start up. Default=true
[create-journal-dir](persistence.md)                                             |  true means that the journal directory will be created. Default=true
[discovery-groups](clusters.md "Clusters")                           |  [a list of discovery-group](#discovery-group-type)
[disk-scan-period](paging.md#max-disk-usage) | The interval where the disk is scanned for percentual usage. Default=5000 ms.
[diverts](diverts.md "Diverting and Splitting Message Flows")        |  [a list of diverts to use](#divert-type)
[global-max-size](paging.md#global-max-size) | The amount in bytes before all addresses are considered full. Default is half of the memory used by the JVM (-Xmx argument).
[graceful-shutdown-enabled](graceful-shutdown.md "Graceful Server Shutdown")      |  true means that graceful shutdown is enabled. Default=false
[graceful-shutdown-timeout](graceful-shutdown.md "Graceful Server Shutdown")      |  Timeout on waiting for clients to disconnect before server shutdown. Default=-1
[grouping-handler](message-grouping.md "Message Grouping")             |  Message Group configuration
[id-cache-size](duplicate-detection.md "Configuring the Duplicate ID Cache")  |  The duplicate detection circular cache size. Default=20000
[jmx-domain](management.md "Configuring JMX")                               |  the JMX domain used to registered MBeans in the MBeanServer. Default=org.apache.activemq
[jmx-management-enabled](management.md "Configuring JMX")                   |  true means that the management API is available via JMX. Default=true
[journal-buffer-size](persistence.md)                                               |  The size of the internal buffer on the journal in KB. Default=490 KiB
[journal-buffer-timeout](persistence.md)                                            |  The Flush timeout for the journal buffer
[journal-compact-min-files](persistence.md)                                         |  The minimal number of data files before we can start compacting. Setting this to 0 means compacting is disabled. Default=10
[journal-compact-percentage](persistence.md)                                        |  The percentage of live data on which we consider compacting the journal. Default=30
[journal-directory](persistence.md)                                                 |  the directory to store the journal files in. Default=data/journal
[journal-file-size](persistence.md)                                                 |  the size (in bytes) of each journal file. Default=10485760 (10 MB)
[journal-max-io](persistence.md#configuring-the-message-journal)           |  the maximum number of write requests that can be in the AIO queue at any one time. Default is 4096 for AIO and 1 for NIO, ignored for MAPPED.
[journal-min-files](persistence.md#configuring-the-message-journal)     |  how many journal files to pre-create. Default=2
[journal-pool-files](persistence.md#configuring-the-message-journal)     |  The upper threshold of the journal file pool,-1 (default) means no Limit. The system will create as many files as needed however when reclaiming files it will shrink back to the `journal-pool-files`
[journal-sync-non-transactional](persistence.md)                                      |  if true wait for non transaction data to be synced to the journal before returning response to client. Default=true
[journal-sync-transactional](persistence.md)                                          |  if true wait for transaction data to be synchronized to the journal before returning response to client. Default=true
[journal-type](persistence.md)                                                        |  the type of journal to use. Default=ASYNCIO
[journal-datasync](persistence.md)                                                        |  It will use msync/fsync on journal operations. Default=true.
[large-messages-directory](large-messages.md "Configuring the server")          |  the directory to store large messages. Default=data/largemessages
[management-address](management.md "Configuring Core Management")   |  the name of the management address to send management messages to. Default=activemq.management
[management-notification-address](management.md "Configuring The Core Management Notification Address") |  the name of the address that consumers bind to receive management notifications. Default=activemq.notifications
[mask-password](masking-passwords.md "Masking Passwords")  |  This option controls whether passwords in server configuration need be masked. If set to "true" the passwords are masked. Default=false
[max-saved-replicated-journals-size](ha.md#data-replication)                                                                |    This specifies how many times a replicated backup server can restart after moving its files on start. Once there are this number of backup journal files the server will stop permanently after if fails back. -1 Means no Limit, 0 don't keep a copy at all, Default=2
[max-disk-usage](paging.md#max-disk-usage) | The max percentage of data we should use from disks. The System will block while the disk is full. Disable by setting -1. Default=100
[memory-measure-interval](perf-tuning.md)                                                             |  frequency to sample JVM memory in ms (or -1 to disable memory sampling). Default=-1
[memory-warning-threshold](perf-tuning.md)                                                            |  Percentage of available memory which will trigger a warning log. Default=25
[message-counter-enabled](management.md "Configuring Message Counters")                       |  true means that message counters are enabled. Default=false
[message-counter-max-day-history](management.md "Configuring Message Counters")               |  how many days to keep message counter history. Default=10 (days)
[message-counter-sample-period](management.md "Configuring Message Counters")                 |  the sample period (in ms) to use for message counters. Default=10000
[message-expiry-scan-period](message-expiry.md "Configuring The Expiry Reaper Thread")          |  how often (in ms) to scan for expired messages. Default=30000
[message-expiry-thread-priority](message-expiry.md "Configuring The Expiry Reaper Thread")      |  the priority of the thread expiring messages. Default=3
[password-codec](masking-passwords.md "Masking Passwords")                                      |  the name of the class (and optional configuration properties) used to decode masked passwords. Only valid when `mask-password` is `true`. Default=empty
[page-max-concurrent-io](paging.md "Paging Mode")                                               |  The max number of concurrent reads allowed on paging. Default=5
[paging-directory](paging.md "Configuration")                                                   |  the directory to store paged messages in. Default=data/paging
[persist-delivery-count-before-delivery](undelivered-messages.md "Delivery Count Persistence")  |  True means that the delivery count is persisted before delivery. False means that this only happens after a message has been cancelled. Default=false
[persistence-enabled](persistence.md "Configuring ActiveMQ Artemis for Zero Persistence")               |  true means that the server will use the file based journal for persistence. Default=true
[persist-id-cache](duplicate-detection.md "Configuring the Duplicate ID Cache")                 |  true means that ID's are persisted to the journal. Default=true
[queues](address-model.md "Predefined Queues")       |  [a list of queue to be created](#queue-type)
[remoting-incoming-interceptors](intercepting-operations.md "Intercepting Operations")                                                   |  A list of interceptor
[resolveProtocols]()  |  Use [ServiceLoader](https://docs.oracle.com/javase/tutorial/ext/basics/spi.html) to load protocol modules. Default=true
[scheduled-thread-pool-max-size](thread-pooling.md#server-scheduled-thread-pool "Server Scheduled Thread Pool")|  Maximum number of threads to use for the scheduled thread pool. Default=5
[security-enabled](security.md "Security")  |  true means that security is enabled. Default=true
[security-invalidation-interval](security.md "Security")                                   |  how long (in ms) to wait before invalidating the security cache. Default=10000
system-property-prefix | Prefix for replacing configuration settings using Bean Utils.
[populate-validated-user](security.md "Security")                                          |  whether or not to add the name of the validated user to the messages that user sends. Default=false
[security-settings](security.md "Role based security for addresses")                             |  [a list of security-setting](#security-setting-type)
[thread-pool-max-size](thread-pooling.md "Server Scheduled Thread Pool")                       |  Maximum number of threads to use for the thread pool. -1 means 'no limits'.. Default=30
[transaction-timeout](transaction-config.md "Resource Manager Configuration")              |  how long (in ms) before a transaction can be removed from the resource manager after create time. Default=300000
[transaction-timeout-scan-period](transaction-config.md "Resource Manager Configuration")  |  how often (in ms) to scan for timeout transactions. Default=1000
[wild-card-routing-enabled](wildcard-routing.md "Routing Messages With Wild Cards")        |  true means that the server supports wild card routing. Default=true
[network-check-NIC](network-isolation.md) | The NIC (Network Interface Controller) to be used on InetAddress.isReachable
[network-check-URL](network-isolation.md) | The list of http URIs to be used to validate the network
[network-check-list](network-isolation.md) | The list of pings to be used on ping or InetAddress.isReachable
[network-check-ping-command](network-isolation.md) | The command used to oping IPV4 addresses
[network-check-ping6-command](network-isolation.md) | The command used to oping IPV6 addresses
[critical-analyzer](critical-analysis.md) | Enable or disable the critical analysis (default true)
[critical-analyzer-timeout](critical-analysis.md) | Timeout used to do the critical analysis (default 120000 milliseconds)
[critical-analyzer-check-period](critical-analysis.md) | Time used to check the response times (default half of critical-analyzer-timeout)
[critical-analyzer-policy](critical-analysis.md) | Should the server log, be halted or shutdown upon failures (default `LOG`)


#address-setting type

Name | Description
:--- | :---
[match ](address-model.md "Configuring Queues Via Address Settings")         | The filter to apply to the setting
[dead-letter-address](undelivered-messages.md "Configuring Dead Letter Addresses")                |  dead letter address
[expiry-address](message-expiry.md "Configuring Expiry Addresses")                                  |  expired messages address
[expiry-delay](address-model.md "Configuring Queues Via Address Settings")                       |  expiration time override, -1 don't override with default=-1
[redelivery-delay](undelivered-messages.md "Configuring Delayed Redelivery")                      |  time to redeliver a message (in ms) with default=0
[redelivery-delay-multiplier](address-model.md "Configuring Queues Via Address Settings")        |  multiplier to apply to the "redelivery-delay"
[max-redelivery-delay](address-model.md "Configuring Queues Via Address Settings")               |  Max value for the redelivery-delay
[max-delivery-attempts](undelivered-messages.md "Configuring Dead Letter Addresses")              |  Number of retries before dead letter address, default=10
[max-size-bytes](paging.md "Paging")                                                          |  Limit before paging. -1 = infinite
[page-size-bytes](paging.md "Paging")                                                         |  Size of each file on page, default=10485760
[page-max-cache-size](paging.md "Paging")                                                     |  Maximum number of files cached from paging default=5
[address-full-policy](address-model.md "Configuring Queues Via Address Settings")                |  Model to chose after queue full
[message-counter-history-day-limit](address-model.md "Configuring Queues Via Address Settings")  |  Days to keep in history
[last-value-queue](last-value-queues.md "Last-Value Queues")                                  |  Queue is a last value queue, default=false
[redistribution-delay](clusters.md "Clusters")                                                |  Timeout before redistributing values after no consumers. default=-1
[send-to-dla-on-no-route](address-model.md "Configuring Queues Via Address Settings")            |  Forward messages to DLA when no queues subscribing. default=false


#bridge type

Name | Description
:--- | :---
[name ](core-bridges.md "Core Bridges")          |  unique name
[queue-name](core-bridges.md "Core Bridges")                         |  name of queue that this bridge consumes from
[forwarding-address](core-bridges.md "Core Bridges")                 |  address to forward to. If omitted original address is used
[ha](core-bridges.md "Core Bridges")                                 |  whether this bridge supports fail-over
[filter](core-bridges.md "Core Bridges")         |  optional core filter expression
[transformer-class-name](core-bridges.md "Core Bridges")             |  optional name of transformer class
[min-large-message-size](core-bridges.md "Core Bridges")             |  Limit before message is considered large. default 100KB
[check-period](connection-ttl.md "Detecting Dead Connections")       |  [TTL](https://en.wikipedia.org/wiki/Time_to_live "Time to Live") check period for the bridge. -1 means disabled. default 30000 (ms)
[connection-ttl](connection-ttl.md "Detecting Dead Connections")     |  [TTL](https://en.wikipedia.org/wiki/Time_to_live "Time to Live") for the Bridge. This should be greater than the ping period. default 60000 (ms)
[retry-interval](core-bridges.md "Core Bridges")                     |  period (in ms) between successive retries. default 2000
[retry-interval-multiplier](core-bridges.md "Core Bridges")          |  multiplier to apply to successive retry intervals. default 1
[max-retry-interval](core-bridges.md "Core Bridges")                 |  Limit to the retry-interval growth. default 2000
[reconnect-attempts](core-bridges.md "Core Bridges")                 |  maximum number of retry attempts, -1 means 'no limits'. default -1
[use-duplicate-detection](core-bridges.md "Core Bridges")            |  forward duplicate detection headers?. default true
[confirmation-window-size](core-bridges.md "Core Bridges")           |  number of bytes before confirmations are sent. default 1MB
[producer-window-size](core-bridges.md "Core Bridges")               |  Producer flow control size on the bridge. Default -1 (disabled)
[user](core-bridges.md "Core Bridges")                               |  Username for the bridge, the default is the cluster username
[password](core-bridges.md "Core Bridges")                           |  Password for the bridge, default is the cluster password
[reconnect-attempts-same-node](core-bridges.md "Core Bridges")       |  Number of retries before trying another node. default 10

# broadcast-group type

Name | Type
:--- | :---
[name ](clusters.md "Clusters")   | unique name
[local-bind-address](clusters.md "Clusters")          | local bind address that the datagram socket is bound to
[local-bind-port](clusters.md "Clusters")             | local port to which the datagram socket is bound to
[group-address](clusters.md "Clusters")               | multicast address to which the data will be broadcast
[group-port](clusters.md "Clusters")                  | UDP port number used for broadcasting
[broadcast-period](clusters.md "Clusters")            | period in milliseconds between consecutive broadcasts. default 2000
[jgroups-file](clusters.md)                                       | Name of JGroups configuration file
[jgroups-channel](clusters.md)                                    | Name of JGroups Channel
[connector-ref](clusters.md "Clusters")              |


#cluster-connection type

Name | Description
:--- | :---
[name](clusters.md "Clusters")                                              |   unique name
[address](clusters.md "Clusters")                                                                |   name of the address this cluster connection applies to
[connector-ref](clusters.md "Clusters")                                                          |   Name of the connector reference to use.
[check-period](connection-ttl.md "Detecting Dead Connections")                                   |   The period (in milliseconds) used to check if the cluster connection has failed to receive pings from another server with default = 30000
[connection-ttl](connection-ttl.md "Detecting Dead Connections")                                 |   Timeout for TTL. Default 60000
[min-large-message-size](large-messages.md "Large Messages")                                     |   Messages larger than this are considered large-messages, default=100KB
[call-timeout](clusters.md "Clusters")                                                           |   Time(ms) before giving up on blocked calls. Default=30000
[retry-interval](clusters.md "Clusters")                                                         |   period (in ms) between successive retries. Default=500
[retry-interval-multiplier](clusters.md "Clusters")                                              |   multiplier to apply to the retry-interval. Default=1
[max-retry-interval](clusters.md "Clusters")                                                     |   Maximum value for retry-interval. Default=2000
[reconnect-attempts](clusters.md "Clusters")                                                     |   How many attempts should be made to reconnect after failure. Default=-1
[use-duplicate-detection](clusters.md "Clusters")                                                |   should duplicate detection headers be inserted in forwarded messages?. Default=true
[message-load-balancing](clusters.md "Clusters")                                                 |   how should messages be load balanced? Default=OFF
[max-hops](clusters.md "Clusters")                                                               |   maximum number of hops cluster topology is propagated. Default=1
[confirmation-window-size](client-reconnection.md "Client Reconnection and Session Reattachment")|   The size (in bytes) of the window used for confirming data from the server connected to. Default 1048576
[producer-window-size](clusters.md "Clusters")                                                   |   Flow Control for the Cluster connection bridge. Default -1 (disabled)
[call-failover-timeout](clusters.md "Configuring Cluster Connections")                               |   How long to wait for a reply if in the middle of a fail-over. -1 means wait forever. Default -1
[notification-interval](clusters.md "Clusters")                                                  |   how often the cluster connection will notify the cluster of its existence right after joining the cluster. Default 1000
[notification-attempts](clusters.md "Clusters")                                                  |   how many times this cluster connection will notify the cluster of its existence right after joining the cluster Default 2


#discovery-group type

Name | Description
:--- | :---
[name](clusters.md "Clusters") |  unique name
[group-address](clusters.md "Clusters")                                                                 |  Multicast IP address of the group to listen on
[group-port](clusters.md "Clusters")                                                                    |  UDP port number of the multi cast group
[jgroups-file](clusters.md)                                                                                         |  Name of a JGroups configuration file. If specified, the server uses JGroups for discovery.
[jgroups-channel](clusters.md)                                                                                      |  Name of a JGroups Channel. If specified, the server uses the named channel for discovery.
[refresh-timeout]()                                                                                                 |    Period the discovery group waits after receiving the last broadcast from a particular server before removing that servers connector pair entry from its list. Default=10000
[local-bind-address](clusters.md "Clusters")                                                            |  local bind address that the datagram socket is bound to
[local-bind-port](clusters.md "Clusters")                                                               |  local port to which the datagram socket is bound to. Default=-1
[initial-wait-timeout]()                                                                                            |    time to wait for an initial broadcast to give us at least one node in the cluster. Default=10000

#divert type

Name | Description
:--- | :---
[name](diverts.md "Diverting and Splitting Message Flows")                                           |  unique name
[transformer-class-name](diverts.md "Diverting and Splitting Message Flows")                                               |  an optional class name of a transformer
[exclusive](diverts.md "Diverting and Splitting Message Flows")                                                            |  whether this is an exclusive divert. Default=false
[routing-name](diverts.md "Diverting and Splitting Message Flows")                                                         |  the routing name for the divert
[address](diverts.md "Diverting and Splitting Message Flows")                                                              |  the address this divert will divert from
[forwarding-address](diverts.md "Diverting and Splitting Message Flows")                                                   |  the forwarding address for the divert
[filter](diverts.md "Diverting and Splitting Message Flows")| optional core filter expression


#queue type

Name | Description
:--- | :---
[name ](address-model.md "Predefined Queues")                                                              |  unique name
[address](address-model.md "Predefined Queues")                                                                                |  address for the queue
[filter](address-model.md "Predefined Queues")                                                                                 | optional core filter expression
[durable](address-model.md "Predefined Queues")                                                                                |  whether the queue is durable (persistent). Default=true


#security-setting type

Name | Description
:--- | :---
[match ](security.md "Role based security for addresses")                               |  [address expression](wildcard-syntax.md)
[permission](security.md "Role based security for addresses")                           |
[permission.type ](security.md "Role based security for addresses")                     |  the type of permission
[permission.roles ](security.md "Role based security for addresses")                    |  a comma-separated list of roles to apply the permission to

