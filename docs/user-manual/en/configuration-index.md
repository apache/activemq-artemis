# Configuration Reference

This section is a quick index for looking up configuration. Click on the
element name to go to the specific chapter.

## Broker Configuration

### broker.xml

This is the main core server configuration file which contains the `core`
element. The `core` element contains the main server configuration.

#### Modularising broker.xml

XML XInclude support is provided in `broker.xml` so that you can break your configuration out into separate files.

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
**Note:** if you use `xmllint` to validate the XML against the schema you should enable xinclude flag when running.

```
--xinclude
```

For further information on XInclude see:

[https://www.w3.org/TR/xinclude/](https://www.w3.org/TR/xinclude/)

##### Reloading modular configuration files

Certain changes in `broker.xml` can be picked up at runtime as discussed in the [Configuration Reload](config-reload.md)
chapter. Changes made directly to files which are included in `broker.xml` via `xi:include` will not be automatically
reloaded. For example, if `broker.xml` is including `my-address-settings.xml` and `my-address-settings.xml` is modified
those changes won't be reloaded automatically. To force a reload in this situation there are 2 main options: 

1. Use the `reloadConfiguration` management operation on the `ActiveMQServerControl`.
2. Update the timestamp on `broker.xml` using something like the [touch](https://en.wikipedia.org/wiki/Touch_%28Unix%29)
   command. The next time the broker inspects `broker.xml` for automatic reload it will see the updated timestamp and
   trigger a reload of `broker.xml` and all its included files.

### System properties

It is possible to use System properties to replace some of the configuration properties. If you define a System property starting with "brokerconfig." that will be passed along to Bean Utils and the configuration would be replaced.

To define global-max-size=1000000 using a system property you would have to define this property, for example through java arguments:

```
java -Dbrokerconfig.globalMaxSize=1000000
```

You can also change the prefix through the `broker.xml` by setting:

```
<system-property-prefix>yourprefix</system-property-prefix>
```

This is to help you customize artemis on embedded systems.


## The core configuration

This describes the root of the XML configuration. You will see here also multiple sub-types listed.
For example on the main config you will have bridges and at the [list of bridge](#bridge-type) type we will describe the properties for that configuration.

> **Warning**
>
> The default values listed below are the values which will be used if
> the configuration parameter is **not set** either programmatically or
> via `broker.xml`. Some of these values are set in the `broker.xml`
> which is available out-of-the-box. Any values set in the
> out-of-the-box configuration will override the default values listed
> here. Please consult your specific configuration to know which values
> will actually be used when the broker is running.

Name | Description | Default
---|---|---
[acceptors](configuring-transports.md#acceptors) | a list of remoting acceptors | n/a
[acceptors.acceptor](configuring-transports.md#acceptors) | Each acceptor is composed for just an URL | n/a
[addresses](address-model.md#basic-address-configuration) | [a list of addresses](#address-type) | n/a
[address-settings](address-model.md#configuring-addresses-and-queues-via-address-settings) | [a list of address-setting](#address-setting-type) | n/a
[allow-failback](ha.md#failing-back-to-live-server)| Should stop backup on live restart. | `true`
[amqp-use-core-subscription-naming](amqp.md) | If true uses CORE queue naming convention for AMQP. | `false`
[async-connection-execution-enabled](connection-ttl.md) | If False delivery would be always asynchronous. | `true`
[bindings-directory](persistence.md) | The folder in use for the bindings folder | `data/bindings`
[bridges](core-bridges.md) | [a list of core bridges](#bridge-type) | n/a
[ha-policy](ha.md) | the HA policy of this server | none
[broadcast-groups](clusters.md#broadcast-groups) | [a list of broadcast-group](#broadcast-group-type) | n/a
[broker-plugins](broker-plugins.md) | [a list of broker-plugins](#broker-plugin-type) | n/a
[configuration-file-refresh-period](config-reload.md) | The frequency in milliseconds the configuration file is checked for changes | 5000
[check-for-live-server](ha.md#data-replication)| Used for a live server to verify if there are other nodes with the same ID on the topology | n/a
[cluster-connections](clusters.md#configuring-cluster-connections) | [a list of cluster-connection](#cluster-connection-type) | n/a
[cluster-password](clusters.md) |Cluster password. It applies to all cluster configurations. | n/a
[cluster-user](clusters.md) |Cluster username. It applies to all cluster configurations. | n/a
[connection-ttl-override](connection-ttl.md) |if set, this will override how long (in ms) to keep a connection alive without receiving a ping. -1 disables this setting. | -1
[connection-ttl-check-interval](connection-ttl.md) |how often (in ms) to check connections for ttl violation. | 2000
[connectors.connector](configuring-transports.md) | The URL for the connector. This is a list | n/a
[create-bindings-dir](persistence.md) | true means that the server will create the bindings directory on start up. | `true`
[create-journal-dir](persistence.md)| true means that the journal directory will be created. | `true`
[discovery-groups](clusters.md#discovery-groups)| [a list of discovery-group](#discovery-group-type) | n/a
[disk-scan-period](paging.md#max-disk-usage) | The interval where the disk is scanned for percentual usage. | 5000
[diverts](diverts.md) | [a list of diverts to use](#divert-type) | n/a
[global-max-size](paging.md#global-max-size) | The amount in bytes before all addresses are considered full. | Half of the JVM's `-Xmx`
[graceful-shutdown-enabled](graceful-shutdown.md)| true means that graceful shutdown is enabled. | `false`
[graceful-shutdown-timeout](graceful-shutdown.md)| Timeout on waiting for clients to disconnect before server shutdown. | -1
[grouping-handler](message-grouping.md) | [a message grouping handler](#grouping-handler-type) | n/a
[id-cache-size](duplicate-detection.md#configuring-the-duplicate-id-cache) | The duplicate detection circular cache size. | 20000
[jmx-domain](management.md#configuring-jmx) | the JMX domain used to registered MBeans in the MBeanServer. | `org.apache.activemq`
[jmx-use-broker-name](management.md#configuring-jmx) | whether or not to use the broker name in the JMX properties. | `true`
[jmx-management-enabled](management.md#configuring-jmx) | true means that the management API is available via JMX. | `true`
[journal-buffer-size](persistence.md#configuring-the-message-journal) | The size of the internal buffer on the journal in KB. | 490KB
[journal-buffer-timeout](persistence.md#configuring-the-message-journal) | The Flush timeout for the journal buffer | 500000 for ASYNCIO; 3333333 for NIO
[journal-compact-min-files](persistence.md#configuring-the-message-journal) | The minimal number of data files before we can start compacting. Setting this to 0 means compacting is disabled. | 10
[journal-compact-percentage](persistence.md#configuring-the-message-journal) | The percentage of live data on which we consider compacting the journal. | 30
[journal-directory](persistence.md#configuring-the-message-journal) | the directory to store the journal files in. | `data/journal`
[node-manager-lock-directory](persistence.md#configuring-the-message-journal) | the directory to store the node manager lock file. | same of `journal-directory`
[journal-file-size](persistence.md#configuring-the-message-journal) | the size (in bytes) of each journal file. | 10MB
[journal-lock-acquisition-timeout](persistence.md#configuring-the-message-journal) | how long (in ms) to wait to acquire a file lock on the journal. | -1
[journal-max-io](persistence.md#configuring-the-message-journal) | the maximum number of write requests that can be in the ASYNCIO queue at any one time. | 4096 for ASYNCIO; 1 for NIO; ignored for MAPPED
[journal-file-open-timeout](persistence.md#configuring-the-message-journal) | the length of time in seconds to wait when opening a new journal file before timing out and failing. | 5
[journal-min-files](persistence.md#configuring-the-message-journal) | how many journal files to pre-create. | 2
[journal-pool-files](persistence.md#configuring-the-message-journal) | The upper threshold of the journal file pool, -1 means no Limit. The system will create as many files as needed however when reclaiming files it will shrink back to the `journal-pool-files` | -1
[journal-sync-non-transactional](persistence.md#configuring-the-message-journal) | if true wait for non transaction data to be synced to the journal before returning response to client. | `true`
[journal-sync-transactional](persistence.md#configuring-the-message-journal)| if true wait for transaction data to be synchronized to the journal before returning response to client. | `true`
[journal-type](persistence.md#configuring-the-message-journal) | the type of journal to use. | `ASYNCIO`
[journal-datasync](persistence.md#configuring-the-message-journal) | It will use msync/fsync on journal operations. | `true`
[large-messages-directory](large-messages.md) | the directory to store large messages. | `data/largemessages`
log-delegate-factory-class-name | **deprecated** the name of the factory class to use for log delegation. | n/a
[management-address](management.md#configuring-management)| the name of the management address to send management messages to. | `activemq.management`
[management-notification-address](management.md#configuring-the-management-notification-address) | the name of the address that consumers bind to receive management notifications. | `activemq.notifications`
[mask-password](masking-passwords.md) | This option controls whether passwords in server configuration need be masked. If set to "true" the passwords are masked. | `false`
[max-saved-replicated-journals-size](ha.md#data-replication) | This specifies how many times a replicated backup server can restart after moving its files on start. Once there are this number of backup journal files the server will stop permanently after if fails back. -1 Means no Limit; 0 don't keep a copy at all. | 2
[max-disk-usage](paging.md#max-disk-usage) | The max percentage of data we should use from disks. The broker will block while the disk is full. Disable by setting -1. | 90
[memory-measure-interval](perf-tuning.md) | frequency to sample JVM memory in ms (or -1 to disable memory sampling). | -1
[memory-warning-threshold](perf-tuning.md)| Percentage of available memory which will trigger a warning log. | 25
[message-counter-enabled](management.md#message-counters) | true means that message counters are enabled. | `false`
[message-counter-max-day-history](management.md#message-counters)| how many days to keep message counter history. | 10
[message-counter-sample-period](management.md#message-counters) | the sample period (in ms) to use for message counters. | 10000
[message-expiry-scan-period](message-expiry.md#configuring-the-expiry-reaper-thread) | how often (in ms) to scan for expired messages. | 30000
[message-expiry-thread-priority](message-expiry.md#configuring-the-expiry-reaper-thread)| **deprecated** the priority of the thread expiring messages. | 3
[metrics-plugin](metrics.md) | [a plugin to export metrics](#metrics-plugin-type) | n/a
[address-queue-scan-period](address-model.md#configuring-addresses-and-queues-via-address-settings) | how often (in ms) to scan for addresses & queues that should be removed. | 30000
name | node name; used in topology notifications if set. | n/a
[password-codec](masking-passwords.md) | the name of the class (and optional configuration properties) used to decode masked passwords. Only valid when `mask-password` is `true`. | n/a
[page-max-concurrent-io](paging.md) | The max number of concurrent reads allowed on paging. | 5
[page-sync-timeout](paging.md#page-sync-timeout) | The time in nanoseconds a page will be synced. | 3333333 for ASYNCIO; `journal-buffer-timeout` for NIO
[read-whole-page](paging.md) | If true the whole page would be read, otherwise just seek and read while getting message. | `false`
[paging-directory](paging.md#configuration)| the directory to store paged messages in. | `data/paging`
[persist-delivery-count-before-delivery](undelivered-messages.md#delivery-count-persistence) | True means that the delivery count is persisted before delivery. False means that this only happens after a message has been cancelled. | `false`
[persistence-enabled](persistence.md#zero-persistence)| true means that the server will use the file based journal for persistence. | `true`
[persist-id-cache](duplicate-detection.md#configuring-the-duplicate-id-cache) | true means that ID's are persisted to the journal. | `true`
queues | **deprecated** [use addresses](#address-type) | n/a
[remoting-incoming-interceptors](intercepting-operations.md)| a list of &lt;class-name/&gt; elements with the names of classes to use for intercepting incoming remoting packets | n/a
[remoting-outgoing-interceptors](intercepting-operations.md)| a list of &lt;class-name/&gt; elements with the names of classes to use for intercepting outgoing remoting packets | n/a
[resolveProtocols]() | Use [ServiceLoader](https://docs.oracle.com/javase/tutorial/ext/basics/spi.html) to load protocol modules. | `true`
[resource-limit-settings](resource-limits.md) | [a list of resource-limits](#resource-limit-type) | n/a
[scheduled-thread-pool-max-size](thread-pooling.md#server-scheduled-thread-pool)| Maximum number of threads to use for the scheduled thread pool. | 5
[security-enabled](security.md) | true means that security is enabled. | `true`
[security-invalidation-interval](security.md) | how long (in ms) to wait before invalidating the security cache. | 10000
system-property-prefix | Prefix for replacing configuration settings using Bean Utils. | n/a
internal-naming-prefix | the prefix used when naming the internal queues and addresses required for implementing certain behaviours. | `$.activemq.internal`
[populate-validated-user](security.md#tracking-the-validated-user)| whether or not to add the name of the validated user to the messages that user sends. | `false`
[security-settings](security.md#role-based-security-for-addresses) | [a list of security-setting](#security-setting-type). | n/a
[thread-pool-max-size](thread-pooling.md#thread-management) | Maximum number of threads to use for the thread pool. -1 means 'no limits'. | 30
[transaction-timeout](transaction-config.md) | how long (in ms) before a transaction can be removed from the resource manager after create time. | 300000
[transaction-timeout-scan-period](transaction-config.md) | how often (in ms) to scan for timeout transactions. | 1000
[wild-card-routing-enabled](wildcard-routing.md) | true means that the server supports wild card routing. | `true`
[network-check-NIC](network-isolation.md) | the NIC (Network Interface Controller) to be used on InetAddress.isReachable. | n/a
[network-check-URL-list](network-isolation.md) | the list of http URIs to be used to validate the network. | n/a
[network-check-list](network-isolation.md) | the list of pings to be used on ping or InetAddress.isReachable. | n/a
[network-check-period](network-isolation.md) | a frequency in milliseconds to how often we should check if the network is still up. | 10000
[network-check-timeout](network-isolation.md) | a timeout used in milliseconds to be used on the ping. | 1000
[network-check-ping-command](network-isolation.md) | the command used to oping IPV4 addresses. | n/a
[network-check-ping6-command](network-isolation.md) | the command used to oping IPV6 addresses. | n/a
[critical-analyzer](critical-analysis.md) | enable or disable the critical analysis. | `true`
[critical-analyzer-timeout](critical-analysis.md) | timeout used to do the critical analysis. | 120000 ms
[critical-analyzer-check-period](critical-analysis.md) | time used to check the response times. | 0.5 \* `critical-analyzer-timeout`
[critical-analyzer-policy](critical-analysis.md) | should the server log, be halted or shutdown upon failures. | `LOG`
resolve-protocols | if true then the broker will make use of any protocol managers that are in available on the classpath, otherwise only the core protocol will be available, unless in embedded mode where users can inject their own protocol managers. | `true`
[resource-limit-settings](resource-limits.md) | [a list of resource-limit](#resource-limit-type). | n/a
server-dump-interval | interval to log server specific information (e.g. memory usage etc). | -1
store | the store type used by the server. | n/a
[wildcard-addresses](wildcard-syntax.md) | parameters to configure wildcard address matching format. | n/a

## address-setting type

Name | Description | Default
---|---|---
[match](address-model.md) | The filter to apply to the setting | n/a
[dead-letter-address](undelivered-messages.md) | Dead letter address | n/a
[auto-create-dead-letter-resources](undelivered-messages.md) | Whether or not to auto-create dead-letter address and/or queue | `false`
[dead-letter-queue-prefix](undelivered-messages.md) | Prefix to use for auto-created dead-letter queues | `DLQ.`
[dead-letter-queue-suffix](undelivered-messages.md) | Suffix to use for auto-created dead-letter queues | `` (empty)
[expiry-address](message-expiry.md) | Expired messages address | n/a
[expiry-delay](message-expiry.md) | Expiration time override; -1 don't override | -1
[redelivery-delay](undelivered-messages.md) | Time to wait before redelivering a message | 0
[redelivery-delay-multiplier](undelivered-messages.md) | Multiplier to apply to the `redelivery-delay` | 1.0
[redelivery-collision-avoidance-factor](undelivered-messages.md) | an additional factor used to calculate an adjustment to the `redelivery-delay` (up or down) | 0.0
[max-redelivery-delay](undelivered-messages.md) | Max value for the `redelivery-delay` | 10 \* `redelivery-delay`
[max-delivery-attempts](undelivered-messages.md)| Number of retries before dead letter address| 10
[max-size-bytes](paging.md)| Max size a queue can be before invoking `address-full-policy` | -1
[max-size-bytes-reject-threshold]() | Used with `BLOCK`, the max size an address can reach before messages are rejected; works in combination with `max-size-bytes` **for AMQP clients only**. | -1
[page-size-bytes](paging.md) | Size of each file on page | 10485760
[page-max-cache-size](paging.md) | Maximum number of files cached from paging | 5
[address-full-policy](address-model.md)| What to do when a queue reaches `max-size-bytes` | `PAGE`
[message-counter-history-day-limit](address-model.md) | Days to keep message counter data | 0
[last-value-queue](last-value-queues.md) | **deprecated** Queue is a last value queue; see `default-last-value-queue` instead | `false`
[default-last-value-queue](last-value-queues.md)| `last-value` value if none is set on the queue | `false`
[default-last-value-key](last-value-queues.md)| `last-value-key` value if none is set on the queue | `null`
[default-exclusive-queue](exclusive-queues.md) | `exclusive` value if none is set on the queue | `false`
[default-non-destructive](exclusive-queues.md) | `non-destructive` value if none is set on the queue | `false`
[default-consumers-before-dispatch](exclusive-queues.md) | `consumers-before-dispatch` value if none is set on the queue | 0
[default-delay-before-dispatch](exclusive-queues.md) | `delay-before-dispatch` value if none is set on the queue | -1
[redistribution-delay](clusters.md) | Timeout before redistributing values after no consumers | -1
[send-to-dla-on-no-route](address-model.md) | Forward messages to DLA when no queues subscribing | `false`
[slow-consumer-threshold](slow-consumers.md) | Min rate of msgs/sec consumed before a consumer is considered "slow" | -1
[slow-consumer-policy](slow-consumers.md) | What to do when "slow" consumer is detected | `NOTIFY`
[slow-consumer-check-period](slow-consumers.md) | How often to check for "slow" consumers | 5
[auto-create-jms-queues](address-model.md#configuring-addresses-and-queues-via-address-settings)| **deprecated** Create JMS queues automatically; see `auto-create-queues` & `auto-create-addresses` | `true`
[auto-delete-jms-queues](address-model.md#configuring-addresses-and-queues-via-address-settings)| **deprecated** Delete JMS queues automatically; see `auto-create-queues` & `auto-create-addresses` | `true`
[auto-create-jms-topics](address-model.md#configuring-addresses-and-queues-via-address-settings)| **deprecated** Create JMS topics automatically; see `auto-create-queues` & `auto-create-addresses` | `true`
[auto-delete-jms-topics](address-model.md#configuring-addresses-and-queues-via-address-settings)| **deprecated** Delete JMS topics automatically; see `auto-create-queues` & `auto-create-addresses` | `true`
[auto-create-queues](address-model.md#configuring-addresses-and-queues-via-address-settings) | Create queues automatically | `true`
[auto-delete-queues](address-model.md#configuring-addresses-and-queues-via-address-settings) | Delete auto-created queues automatically | `true`
[auto-delete-created-queues](address-model.md#configuring-addresses-and-queues-via-address-settings) | Delete created queues automatically | `false`
[auto-delete-queues-delay](address-model.md#configuring-addresses-and-queues-via-address-settings) | Delay for deleting auto-created queues | 0
[auto-delete-queues-message-count](address-model.md#configuring-addresses-and-queues-via-address-settings) | Message count the queue must be at or below before it can be auto deleted | 0
[config-delete-queues](config-reload.md)| How to deal with queues deleted from XML at runtime| `OFF`
[auto-create-addresses](address-model.md#configuring-addresses-and-queues-via-address-settings) | Create addresses automatically | `true`
[auto-delete-addresses](address-model.md#configuring-addresses-and-queues-via-address-settings) | Delete auto-created addresses automatically | `true`
[auto-delete-addresses-delay](address-model.md#configuring-addresses-and-queues-via-address-settings) | Delay for deleting auto-created addresses | 0
[config-delete-addresses](config-reload.md) | How to deal with addresses deleted from XML at runtime | `OFF`
[config-delete-diverts](config-reload.md) | How to deal with diverts deleted from XML at runtime | `OFF`
[management-browse-page-size]() | Number of messages a management resource can browse| 200
[default-purge-on-no-consumers](address-model.md#non-durable-subscription-queue) | `purge-on-no-consumers` value if none is set on the queue | `false`
[default-max-consumers](address-model.md#shared-durable-subscription-queue-using-max-consumers) | `max-consumers` value if none is set on the queue | -1
[default-queue-routing-type](address-model.md#routing-type) | Routing type for auto-created queues if the type can't be otherwise determined | `MULTICAST`
[default-address-routing-type](address-model.md#routing-type) | Routing type for auto-created addresses if the type can't be otherwise determined | `MULTICAST`
[default-ring-size](ring-queues.md) | The ring-size applied to queues without an explicit `ring-size` configured | `-1`
[retroactive-message-count](retroactive-addresses.md) | the number of messages to preserve for future queues created on the matching address | `0`


## bridge type

Name | Description | Default
---|---|---
[name ](core-bridges.md)| unique name | n/a
[queue-name](core-bridges.md) | name of queue that this bridge consumes from | n/a
[forwarding-address](core-bridges.md) | address to forward to. If omitted original address is used | n/a
[ha](core-bridges.md)| whether this bridge supports fail-over | `false`
[filter](core-bridges.md) | optional core filter expression | n/a
[transformer-class-name](core-bridges.md) | optional name of transformer class | n/a
[min-large-message-size](core-bridges.md) | Limit before message is considered large. | 100KB
[check-period](connection-ttl.md)| How often to check for [TTL](https://en.wikipedia.org/wiki/Time_to_live) violation. -1 means disabled. | 30000
[connection-ttl](connection-ttl.md) | [TTL](https://en.wikipedia.org/wiki/Time_to_live "Time to Live") for the Bridge. This should be greater than the ping period. | 60000
[retry-interval](core-bridges.md)| period (in ms) between successive retries. | 2000
[retry-interval-multiplier](core-bridges.md) | multiplier to apply to successive retry intervals. | 1
[max-retry-interval](core-bridges.md) | Limit to the retry-interval growth. | 2000
[reconnect-attempts](core-bridges.md) | maximum number of retry attempts.| -1 (no limit)
[use-duplicate-detection](core-bridges.md)| forward duplicate detection headers? | `true`
[confirmation-window-size](core-bridges.md) | number of bytes before confirmations are sent. | 1MB
[producer-window-size](core-bridges.md)| Producer flow control size on the bridge. | -1 (disabled)
[user](core-bridges.md) | Username for the bridge, the default is the cluster username. | n/a
[password](core-bridges.md)| Password for the bridge, default is the cluster password. | n/a
[reconnect-attempts-same-node](core-bridges.md) | Number of retries before trying another node. | 10
[routing-type](core-bridges.md) | how to set the routing-type on the bridged message | `PASS`
[concurrency](core-bridges.md) | Concurrency of the bridge | 1

## broadcast-group type

Name | Type
---|---
[name ](clusters.md) | unique name
[local-bind-address](clusters.md) | Local bind address that the datagram socket is bound to.
[local-bind-port](clusters.md) | Local port to which the datagram socket is bound to.
[group-address](clusters.md)| Multicast address to which the data will be broadcast.
[group-port](clusters.md)| UDP port number used for broadcasting.
[broadcast-period](clusters.md)| Period in milliseconds between consecutive broadcasts. Default=2000.
[jgroups-file](clusters.md) | Name of JGroups configuration file.
[jgroups-channel](clusters.md) | Name of JGroups Channel.
[connector-ref](clusters.md)| The `connector` to broadcast.


## cluster-connection type

Name | Description | Default
---|---|---
[name](clusters.md) | unique name | n/a
[address](clusters.md) | name of the address this cluster connection applies to | n/a
[connector-ref](clusters.md) | Name of the connector reference to use. | n/a
[check-period](connection-ttl.md) | The period (in milliseconds) used to check if the cluster connection has failed to receive pings from another server | 30000
[connection-ttl](connection-ttl.md)| Timeout for TTL. | 60000
[min-large-message-size](large-messages.md) | Messages larger than this are considered large-messages. | 100KB
[call-timeout](clusters.md) | Time(ms) before giving up on blocked calls. | 30000
[retry-interval](clusters.md)| period (in ms) between successive retries. | 500
[retry-interval-multiplier](clusters.md) | multiplier to apply to the retry-interval. | 1
[max-retry-interval](clusters.md) | Maximum value for retry-interval. | 2000
[reconnect-attempts](clusters.md) | How many attempts should be made to reconnect after failure. | -1
[use-duplicate-detection](clusters.md)| should duplicate detection headers be inserted in forwarded messages? | `true`
[message-load-balancing](clusters.md) | how should messages be load balanced? | `OFF`
[max-hops](clusters.md)| maximum number of hops cluster topology is propagated. | 1
[confirmation-window-size](client-reconnection.md#client-reconnection-and-session-reattachment)| The size (in bytes) of the window used for confirming data from the server connected to. | 1048576
[producer-window-size](clusters.md)| Flow Control for the Cluster connection bridge. | -1 (disabled)
[call-failover-timeout](clusters.md#configuring-cluster-connections)| How long to wait for a reply if in the middle of a fail-over. -1 means wait forever. | -1
[notification-interval](clusters.md) | how often the cluster connection will notify the cluster of its existence right after joining the cluster. | 1000
[notification-attempts](clusters.md) | how many times this cluster connection will notify the cluster of its existence right after joining the cluster | 2


## discovery-group type

Name | Description
---|---
[name](clusters.md)| unique name
[group-address](clusters.md)| Multicast IP address of the group to listen on
[group-port](clusters.md)| UDP port number of the multi cast group
[jgroups-file](clusters.md) | Name of a JGroups configuration file. If specified, the server uses JGroups for discovery.
[jgroups-channel](clusters.md) | Name of a JGroups Channel. If specified, the server uses the named channel for discovery.
[refresh-timeout]()| Period the discovery group waits after receiving the last broadcast from a particular server before removing that servers connector pair entry from its list. Default=10000
[local-bind-address](clusters.md) | local bind address that the datagram socket is bound to
[local-bind-port](clusters.md) | local port to which the datagram socket is bound to. Default=-1
initial-wait-timeout | time to wait for an initial broadcast to give us at least one node in the cluster. Default=10000

## divert type

Name | Description
---|---
[name](diverts.md) | unique name
[transformer-class-name](diverts.md) | an optional class name of a transformer
[exclusive](diverts.md) | whether this is an exclusive divert. Default=false
[routing-name](diverts.md) | the routing name for the divert
[address](diverts.md) | the address this divert will divert from
[forwarding-address](diverts.md) | the forwarding address for the divert
[filter](diverts.md) | optional core filter expression
[routing-type](diverts.md) | how to set the routing-type on the diverted message. Default=`STRIP`


## address type

Name | Description
---|---
name | unique name | n/a
[anycast](address-model.md)| list of anycast [queues](#queue-type)
[multicast](address-model.md) | list of multicast [queues](#queue-type)


## queue type

Name | Description | Default
---|---|---
name | unique name | n/a
filter | optional core filter expression | n/a
durable | whether the queue is durable (persistent). | `true`
user | the name of the user to associate with the creation of the queue | n/a
[max-consumers](address-model.md#shared-durable-subscription-queue-using-max-consumers) | the max number of consumers allowed on this queue | -1 (no max)
[purge-on-no-consumers](address-model.md#non-durable-subscription-queue) | whether or not to delete all messages and prevent routing when no consumers are connected | `false`
[exclusive](exclusive-queues.md) | only deliver messages to one of the connected consumers | `false`
[last-value](last-value-queues.md) | use last-value semantics | `false`
[ring-size](ring-queues.md) | the size this queue should maintain according to ring semantics | based on `default-ring-size` `address-setting`
consumers-before-dispatch | number of consumers required before dispatching messages | 0
delay-before-dispatch | milliseconds to wait for `consumers-before-dispatch` to be met before dispatching messages anyway | -1 (wait forever)


## security-setting type

Name | Description
---|---
[match](security.md)| [address expression](wildcard-syntax.md)
[permission](security.md) |
[permission.type](security.md) | the type of permission
[permission.roles](security.md) | a comma-separated list of roles to apply the permission to
[role-mapping](security.md) | A simple role mapping that can be used to map roles from external authentication providers (i.e. LDAP) to internal roles
[role-mapping.from](security.md) | The external role which should be mapped
[role-mapping.to](security.md) | The internal role which should be assigned to the authenticated user


## broker-plugin type

Name | Description
---|---
[property](broker-plugins.md#registering-a-plugin)| properties to configure a plugin
[class-name](broker-plugins.md#registering-a-plugin) | the name of the broker plugin class to instantiate


## metrics-plugin type

Name | Description
---|---
[property](metrics.md)| properties to configure a plugin
[class-name](metrics.md) | the name of the metrics plugin class to instantiate


## resource-limit type

Name | Description | Default
---|---|---
[match](resource-limits.md#configuring-limits-via-resource-limit-settings)| the name of the user to whom the limits should be applied | n/a
[max-connections](resource-limits.md#configuring-limits-via-resource-limit-settings)| how many connections are allowed by the matched user | -1 (no max)
[max-queues](resource-limits.md#configuring-limits-via-resource-limit-settings)| how many queues can be created by the matched user | -1 (no max)


## grouping-handler type

Name | Description | Default
---|---|---
[name](message-grouping.md#clustered-grouping) | A unique name | n/a
[type](message-grouping.md#clustered-grouping) | `LOCAL` or `REMOTE` | n/a
[address](message-grouping.md#clustered-grouping) | A reference to a `cluster-connection` `address` | n/a
[timeout](message-grouping.md#clustered-grouping) | How long to wait for a decision | 5000
[group-timeout](message-grouping.md#clustered-grouping) | How long a group binding will be used. | -1 (disabled)
[reaper-period](message-grouping.md#clustered-grouping) | How often the reaper will be run to check for timed out group bindings. Only valid for `LOCAL` handlers. | 30000
