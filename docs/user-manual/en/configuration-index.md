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

This describes the root of the XML configuration. You will see here also multiple sub-types listed.
For example on the main config you will have bridges and at the [list of bridge](#bridge-type) type we will describe the properties for that configuration.

Name | Description
:--- | :---
[acceptors](configuring-transports.md "16.1. Understanding Acceptors") | a list of remoting acceptors
[acceptors.acceptor](configuring-transports.md "16.1. Understanding Acceptors") | Each acceptor is composed for just an URL
[address-settings](queue-attributes.md "25.3. Configuring Queues Via Address Settings")                                                    |  [a list of address-setting](#address-setting-type)
[allow-failback](ha.md "39.1.4. Failing Back to live Server")                                                                              |  Should stop backup on live restart. default true
[async-connection-execution-enabled](connection-ttl.md "17.3. Configuring Asynchronous Connection Execution")  | If False delivery would be always asynchronous. default true
[bindings-directory](persistence.md "15.1. Configuring the bindings journal")  | The folder in use for the bindings folder
[bridges](core-bridges.md "Chapter 36. Core Bridges")  | [a list of bridge](#bridge-type)
[broadcast-groups](clusters.md "Chapter 38. Clusters")                                            | [a list of broadcast-group](#broadcast-group-type)
[check-for-live-server](ha.md)   |  Used for a live server to verify if there are other nodes with the same ID on the topology
[cluster-connections](clusters.md "Chapter 38. Clusters") |  [a list of cluster-connection](#cluster-connection-type)
[cluster-password](clusters.md "Chapter 38. Clusters")                                                                                              |   Cluster password. It applies to all cluster configurations.
[cluster-user](clusters.md "Chapter 38. Clusters")                                                                                                  |   Cluster username. It applies to all cluster configurations.
[connection-ttl-override](connection-ttl.md)                                                                                                        |   if set, this will override how long (in ms) to keep a connection alive without receiving a ping. -1 disables this setting. Default -1
[connectors.connector](configuring-transports.md "16.2. Understanding Connectors") | The URL for the connector. This is a list
[create-bindings-dir](persistence.md "15.1. Configuring the bindings journal") |  true means that the server will create the bindings directory on start up. Default=true
[create-journal-dir](persistence.md)                                             |  true means that the journal directory will be created. Default=true
[discovery-groups](clusters.md "Chapter 38. Clusters")                           |  [a list of discovery-group](#discovery-group-type)
[diverts](diverts.md "Chapter 35. Diverting and Splitting Message Flows")        |  [a list of diverts to use](#divert-type)
[graceful-shutdown-enabled](graceful-shutdown.md "Graceful Server Shutdown")      |  true means that graceful shutdown is enabled. Default=true
[graceful-shutdown-timeout](graceful-shutdown.md "Graceful Server Shutdown")      |  Timeout on waitin for clients to disconnect before server shutdown. Default=-1
[grouping-handler](message-grouping.md "Chapter 28. Message Grouping")             |  Message Group configuration
[id-cache-size](duplicate-detection.md "37.2. Configuring the Duplicate ID Cache")  |  The duplicate detection circular cache size. Default=20000
[jmx-domain](management.md "30.2.1. Configuring JMX")                               |  the JMX domain used to registered MBeans in the MBeanServer. Default=org.apache.activemq
[jmx-management-enabled](management.md "30.2.1. Configuring JMX")                   |  true means that the management API is available via JMX. Default=true
[journal-buffer-size](persistence.md)                                               |  The size of the internal buffer on the journal in KB. Default=490 KiB
[journal-buffer-timeout](persistence.md)                                            |  The Flush timeout for the journal buffer
[journal-compact-min-files](persistence.md)                                         |  The minimal number of data files before we can start compacting. Setting this to 0 means compacting is disabled. Default=10
[journal-compact-percentage](persistence.md)                                        |  The percentage of live data on which we consider compacting the journal. Default=30
[journal-directory](persistence.md)                                                 |  the directory to store the journal files in. Default=data/journal
[journal-file-size](persistence.md)                                                 |  the size (in bytes) of each journal file. Default=10485760 (10 MB)
[journal-max-io](persistence.md#configuring.message.journal.journal-max-io)           |  the maximum number of write requests that can be in the AIO queue at any one time. Default is 500 for AIO and 1 for NIO.
[journal-min-files](persistence.md#configuring.message.journal.journal-min-files)     |  how many journal files to pre-create. Default=2
[journal-sync-non-transactional](persistence.md)                                      |  if true wait for non transaction data to be synced to the journal before returning response to client. Default=true
[journal-sync-transactional](persistence.md)                                          |  if true wait for transaction data to be synchronized to the journal before returning response to client. Default=true
[journal-type](persistence.md)                                                        |  the type of journal to use. Default=ASYNCIO
[large-messages-directory](large-messages.md "23.1. Configuring the server")          |  the directory to store large messages. Default=data/largemessages
[management-address](management.md "30.3.1. Configuring Core Management")   |  the name of the management address to send management messages to. It is prefixed with "jms.queue" so that JMS clients can send messages to it. Default=jms.queue.activemq.management
[management-notification-address](management.md "30.5.2.1. Configuring The Core Management Notification Address") |  the name of the address that consumers bind to receive management notifications. Default=activemq.notifications
[mask-password](configuration-index.md "50.1.3. Using Masked Passwords in Configuration Files")  |  This option controls whether passwords in server configuration need be masked. If set to "true" the passwords are masked. Default=false
[max-saved-replicated-journals-size]()                                                                |    This specifies how many times a replicated backup server can restart after moving its files on start. Once there are this number of backup journal files the server will stop permanently after if fails back. Default=2
[memory-measure-interval](perf-tuning.md)                                                             |  frequency to sample JVM memory in ms (or -1 to disable memory sampling). Default=-1
[memory-warning-threshold](perf-tuning.md)                                                            |  Percentage of available memory which will trigger a warning log. Default=25
[message-counter-enabled](management.md "30.6.1. Configuring Message Counters")                       |  true means that message counters are enabled. Default=false
[message-counter-max-day-history](management.md "30.6.1. Configuring Message Counters")               |  how many days to keep message counter history. Default=10 (days)
[message-counter-sample-period](management.md "30.6.1. Configuring Message Counters")                 |  the sample period (in ms) to use for message counters. Default=10000
[message-expiry-scan-period](message-expiry.md "22.3. Configuring The Expiry Reaper Thread")          |  how often (in ms) to scan for expired messages. Default=30000
[message-expiry-thread-priority](message-expiry.md "22.3. Configuring The Expiry Reaper Thread")      |  the priority of the thread expiring messages. Default=3
[page-max-concurrent-io](paging.md "24.3. Paging Mode")                                               |  The max number of concurrent reads allowed on paging. Default=5
[paging-directory](paging.md "24.2. Configuration")                                                   |  the directory to store paged messages in. Default=data/paging
[persist-delivery-count-before-delivery](undelivered-messages.md "21.3. Delivery Count Persistence")  |  True means that the delivery count is persisted before delivery. False means that this only happens after a message has been cancelled. Default=false
[persistence-enabled](persistence.md "15.6. Configuring ActiveMQ for Zero Persistence")               |  true means that the server will use the file based journal for persistence. Default=true
[persist-id-cache](duplicate-detection.md "37.2. Configuring the Duplicate ID Cache")                 |  true means that ID's are persisted to the journal. Default=true
[queues](queue-attributes.md "25.1. Predefined Queues")       |  [a list of queue to be created](#queue-type)
[remoting-incoming-interceptors](intercepting-operations.md "Chapter 47. Intercepting Operations")                                                   |  A list of interceptor
[resolveProtocols]()  |  Use [ServiceLoader](http://docs.oracle.com/javase/tutorial/ext/basics/spi.html) to load protocol modules. Default=true
[scheduled-thread-pool-max-size](thread-pooling.md#server.scheduled.thread.pool "41.1.1. Server Scheduled Thread Pool")|  Maximum number of threads to use for the scheduled thread pool. Default=5
[security-enabled](security.md "Chapter 31. Security")  |  true means that security is enabled. Default=true
[security-invalidation-interval](security.md "Chapter 31. Security")                                   |  how long (in ms) to wait before invalidating the security cache. Default=10000
[security-settings](security.md "31.1. Role based security for addresses")                             |  [a list of security-setting](#security-setting-type)
[thread-pool-max-size](thread-pooling.md "41.1.1. Server Scheduled Thread Pool")                       |  Maximum number of threads to use for the thread pool. -1 means 'no limits'.. Default=30
[transaction-timeout](transaction-config.md "Chapter 18. Resource Manager Configuration")              |  how long (in ms) before a transaction can be removed from the resource manager after create time. Default=300000
[transaction-timeout-scan-period](transaction-config.md "Chapter 18. Resource Manager Configuration")  |  how often (in ms) to scan for timeout transactions. Default=1000
[wild-card-routing-enabled](wildcard-routing.md "Chapter 12. Routing Messages With Wild Cards")        |  true means that the server supports wild card routing. Default=true

#address-setting type

Name | Description
:--- | :---
[match ](queue-attributes.md "25.3. Configuring Queues Via Address Settings")         | The filter to apply to the setting
[dead-letter-address](undelivered-messages.md "21.2.1. Configuring Dead Letter Addresses")                |  dead letter address
[expiry-address](message-expiry.md "22.2. Configuring Expiry Addresses")                                  |  expired messages address
[expiry-delay](queue-attributes.md "25.3. Configuring Queues Via Address Settings")                       |  expiration time override, -1 don't override with default=-1
[redelivery-delay](undelivered-messages.md "21.1.1. Configuring Delayed Redelivery")                      |  time to redeliver a message (in ms) with default=0
[redelivery-delay-multiplier](queue-attributes.md "25.3. Configuring Queues Via Address Settings")        |  multiplier to apply to the "redelivery-delay"
[max-redelivery-delay](queue-attributes.md "25.3. Configuring Queues Via Address Settings")               |  Max value for the redelivery-delay
[max-delivery-attempts](undelivered-messages.md "21.2.1. Configuring Dead Letter Addresses")              |  Number of retries before dead letter address, default=10
[max-size-bytes](paging.md "Chapter 24. Paging")                                                          |  Limit before paging. -1 = infinite
[page-size-bytes](paging.md "Chapter 24. Paging")                                                         |  Size of each file on page, default=10485760
[page-max-cache-size](paging.md "Chapter 24. Paging")                                                     |  Maximum number of files cached from paging default=5
[address-full-policy](queue-attributes.md "25.3. Configuring Queues Via Address Settings")                |  Model to chose after queue full
[message-counter-history-day-limit](queue-attributes.md "25.3. Configuring Queues Via Address Settings")  |  Days to keep in history
[last-value-queue](last-value-queues.md "Chapter 27. Last-Value Queues")                                  |  Queue is a last value queue, default=false
[redistribution-delay](clusters.md "Chapter 38. Clusters")                                                |  Timeout before redistributing values after no consumers. default=-1
[send-to-dla-on-no-route](queue-attributes.md "25.3. Configuring Queues Via Address Settings")            |  Forward messages to DLA when no queues subscribing. default=false


#bridge type

Name | Description
:--- | :---
[name ](core-bridges.md "Chapter 36. Core Bridges")          |  unique name
[queue-name](core-bridges.md "Chapter 36. Core Bridges")                         |  name of queue that this bridge consumes from
[forwarding-address](core-bridges.md "Chapter 36. Core Bridges")                 |  address to forward to. If omitted original address is used
[ha](core-bridges.md "Chapter 36. Core Bridges")                                 |  whether this bridge supports fail-over
[filter](core-bridges.md "Chapter 36. Core Bridges")         |  optional core filter expression                    |
[transformer-class-name](core-bridges.md "Chapter 36. Core Bridges")             |  optional name of transformer class
[min-large-message-size](core-bridges.md "Chapter 36. Core Bridges")             |  Limit before message is considered large. default 100KB
[check-period](connection-ttl.md "Chapter 17. Detecting Dead Connections")       |  [TTL](http://en.wikipedia.org/wiki/Time_to_live "Time to Live") check period for the bridge. -1 means disabled. default 30000 (ms)
[connection-ttl](connection-ttl.md "Chapter 17. Detecting Dead Connections")     |  [TTL](http://en.wikipedia.org/wiki/Time_to_live "Time to Live") for the Bridge. This should be greater than the ping period. default 60000 (ms)
[retry-interval](core-bridges.md "Chapter 36. Core Bridges")                     |  period (in ms) between successive retries. default 2000
[retry-interval-multiplier](core-bridges.md "Chapter 36. Core Bridges")          |  multiplier to apply to successive retry intervals. default 1
[max-retry-interval](core-bridges.md "Chapter 36. Core Bridges")                 |  Limit to the retry-interval growth. default 2000
[reconnect-attempts](core-bridges.md "Chapter 36. Core Bridges")                 |  maximum number of retry attempts, -1 means 'no limits'. default -1
[use-duplicate-detection](core-bridges.md "Chapter 36. Core Bridges")            |  forward duplicate detection headers?. default true
[confirmation-window-size](core-bridges.md "Chapter 36. Core Bridges")           |  number of bytes before confirmations are sent. default 1MB
[producer-window-size](core-bridges.md "Chapter 36. Core Bridges")               |  Producer flow control size on the bridge. Default -1 (disabled)
[user](core-bridges.md "Chapter 36. Core Bridges")                               |  Username for the bridge, the default is the cluster username
[password](core-bridges.md "Chapter 36. Core Bridges")                           |  Password for the bridge, default is the cluster password
[reconnect-attempts-same-node](core-bridges.md "Chapter 36. Core Bridges")       |  Number of retries before trying another node. default 10

# broadcast-group type

Name | Type
:--- | :---
[name ](clusters.md "Chapter 38. Clusters")   | unique name
[local-bind-address](clusters.md "Chapter 38. Clusters")          | local bind address that the datagram socket is bound to
[local-bind-port](clusters.md "Chapter 38. Clusters")             | local port to which the datagram socket is bound to
[group-address](clusters.md "Chapter 38. Clusters")               | multicast address to which the data will be broadcast
[group-port](clusters.md "Chapter 38. Clusters")                  | UDP port number used for broadcasting
[broadcast-period](clusters.md "Chapter 38. Clusters")            | period in milliseconds between consecutive broadcasts. default 2000
[jgroups-file](clusters.md)                                       | Name of JGroups configuration file
[jgroups-channel](clusters.md)                                    | Name of JGroups Channel
[connector-ref](clusters.md "Chapter 38. Clusters")              |


#cluster-connection type

Name | Description
:--- | :---
[name](clusters.md "Chapter 38. Clusters")                                              |   unique name
[address](clusters.md "Chapter 38. Clusters")                                                                |   name of the address this cluster connection applies to
[connector-ref](clusters.md "Chapter 38. Clusters")                                                          |   Name of the connector reference to use.
[check-period](connection-ttl.md "Chapter 17. Detecting Dead Connections")                                   |   The period (in milliseconds) used to check if the cluster connection has failed to receive pings from another server with default = 30000
[connection-ttl](connection-ttl.md "Chapter 17. Detecting Dead Connections")                                 |   Timeout for TTL. Default 60000
[min-large-message-size](large-messages.md "Chapter 23. Large Messages")                                     |   Messages larger than this are considered large-messages, default=100KB
[call-timeout](clusters.md "Chapter 38. Clusters")                                                           |   Time(ms) before giving up on blocked calls. Default=30000
[retry-interval](clusters.md "Chapter 38. Clusters")                                                         |   period (in ms) between successive retries. Default=500
[retry-interval-multiplier](clusters.md "Chapter 38. Clusters")                                              |   multiplier to apply to the retry-interval. Default=1
[max-retry-interval](clusters.md "Chapter 38. Clusters")                                                     |   Maximum value for retry-interval. Default=2000
[reconnect-attempts](clusters.md "Chapter 38. Clusters")                                                     |   How many attempts should be made to reconnect after failure. Default=-1
[use-duplicate-detection](clusters.md "Chapter 38. Clusters")                                                |   should duplicate detection headers be inserted in forwarded messages?. Default=true
[forward-when-no-consumers](clusters.md "Chapter 38. Clusters")                                              |   should messages be load balanced if there are no matching consumers on target? Default=false
[max-hops](clusters.md "Chapter 38. Clusters")                                                               |   maximum number of hops cluster topology is propagated. Default=1
[confirmation-window-size](client-reconnection.md "Chapter 34. Client Reconnection and Session Reattachment")|   The size (in bytes) of the window used for confirming data from the server connected to. Default 1048576
[producer-window-size](clusters.md "Chapter 38. Clusters")                                                   |   Flow Control for the Cluster connection bridge. Default -1 (disabled)
[call-failover-timeout](clusters.md "38.3.1. Configuring Cluster Connections")                               |   How long to wait for a reply if in the middle of a fail-over. -1 means wait forever. Default -1
[notification-interval](clusters.md "Chapter 38. Clusters")                                                  |   how often the cluster connection will notify the cluster of its existence right after joining the cluster. Default 1000
[notification-attempts](clusters.md "Chapter 38. Clusters")                                                  |   how many times this cluster connection will notify the cluster of its existence right after joining the cluster Default 2


#discovery-group type

Name | Description
:--- | :---
[name](clusters.md "Chapter 38. Clusters") |  unique name
[group-address](clusters.md "Chapter 38. Clusters")                                                                 |  Multicast IP address of the group to listen on
[group-port](clusters.md "Chapter 38. Clusters")                                                                    |  UDP port number of the multi cast group
[jgroups-file](clusters.md)                                                                                         |  Name of a JGroups configuration file. If specified, the server uses JGroups for discovery.
[jgroups-channel](clusters.md)                                                                                      |  Name of a JGroups Channel. If specified, the server uses the named channel for discovery.
[refresh-timeout]()                                                                                                 |    Period the discovery group waits after receiving the last broadcast from a particular server before removing that servers connector pair entry from its list. Default=10000
[local-bind-address](clusters.md "Chapter 38. Clusters")                                                            |  local bind address that the datagram socket is bound to
[local-bind-port](clusters.md "Chapter 38. Clusters")                                                               |  local port to which the datagram socket is bound to. Default=-1
[initial-wait-timeout]()                                                                                            |    time to wait for an initial broadcast to give us at least one node in the cluster. Default=10000

#divert type

Name | Description
:--- | :---
[name](diverts.md "Chapter 35. Diverting and Splitting Message Flows")                                           |  unique name
[transformer-class-name](diverts.md "Chapter 35. Diverting and Splitting Message Flows")                                               |  an optional class name of a transformer
[exclusive](diverts.md "Chapter 35. Diverting and Splitting Message Flows")                                                            |  whether this is an exclusive divert. Default=false
[routing-name](diverts.md "Chapter 35. Diverting and Splitting Message Flows")                                                         |  the routing name for the divert
[address](diverts.md "Chapter 35. Diverting and Splitting Message Flows")                                                              |  the address this divert will divert from
[forwarding-address](diverts.md "Chapter 35. Diverting and Splitting Message Flows")                                                   |  the forwarding address for the divert
[filter](diverts.md "Chapter 35. Diverting and Splitting Message Flows")| optional core filter expression


#queue type

Name | Description
:--- | :---
[name ](queue-attributes.md "25.1. Predefined Queues")                                                              |  unique name
[address](queue-attributes.md "25.1. Predefined Queues")                                                                                |  address for the queue
[filter](queue-attributes.md "25.1. Predefined Queues")                                                                                 | optional core filter expression
[durable](queue-attributes.md "25.1. Predefined Queues")                                                                                |  whether the queue is durable (persistent). Default=true


#security-setting type

Name | Description
:--- | :---
[match ](security.md "31.1. Role based security for addresses")                               |  [address expression](wildcard-syntax.md)
[permission](security.md "31.1. Role based security for addresses")                           |
[permission.type ](security.md "31.1. Role based security for addresses")                     |  the type of permission
[permission.roles ](security.md "31.1. Role based security for addresses")                    |  a comma-separated list of roles to apply the permission to

----------------------------

##The jms configuration

Name | Type | Description
:--- | :--- | :---
[queue](using-jms.md "7.2. JMS Server Configuration")                  | Queue     |   a queue
[queue.name (attribute)](using-jms.md "7.2. JMS Server Configuration") | String    |   unique name of the queue
[queue.durable](using-jms.md "7.2. JMS Server Configuration")          | Boolean   |   is the queue durable?. Default=true
[queue.filter](using-jms.md "7.2. JMS Server Configuration")           | String    |   optional filter expression for the queue
[topic](using-jms.md "7.2. JMS Server Configuration")                  | Topic     |   a topic
[topic.name (attribute)](using-jms.md "7.2. JMS Server Configuration") | String    |   unique name of the topic


Using Masked Passwords in Configuration Files
---------------------------------------------

By default all passwords in Apache ActiveMQ Artemis server's configuration files are in
plain text form. This usually poses no security issues as those files
should be well protected from unauthorized accessing. However, in some
circumstances a user doesn't want to expose its passwords to more eyes
than necessary.

Apache ActiveMQ Artemis can be configured to use 'masked' passwords in its
configuration files. A masked password is an obscure string
representation of a real password. To mask a password a user will use an
'encoder'. The encoder takes in the real password and outputs the masked
version. A user can then replace the real password in the configuration
files with the new masked password. When Apache ActiveMQ Artemis loads a masked
password, it uses a suitable 'decoder' to decode it into real password.

Apache ActiveMQ Artemis provides a default password encoder and decoder. Optionally
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

When a Connector or Acceptor configuration is initialised, Apache ActiveMQ Artemis will
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

  mask-password  | cluster-password  | acceptor/connector passwords |  bridge password
  :------------- | :---------------- | :--------------------------- | :---------------
  absent   |       plain text     |    plain text       |              plain text
  false    |       plain text     |    plain text       |              plain text
  true     |       masked         |    masked           |              masked

Examples

Note: In the following examples if related attributed or properties are
absent, it means they are not specified in the configure file.

example 1

```xml
<cluster-password>bbc</cluster-password>
```

This indicates the cluster password is a plain text value ("bbc").

example 2

```xml
<mask-password>true</mask-password>
<cluster-password>80cf731af62c290</cluster-password>
```

This indicates the cluster password is a masked value and Apache ActiveMQ Artemis will
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

```xml
<property name="useMaskedPassword">true</property>
<property name="passwordCodec">com.foo.FooDecoder;key=value</property>
```

Apache ActiveMQ Artemis will load this property and initialize the class with a
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

```xml
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
```

With this configuration, both passwords in ra.xml and all of its MDBs
will have to be in masked form.

### Masking passwords in activemq-users.properties

Apache ActiveMQ Artemis's built-in security manager uses plain properties files
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

```xml
<mask-password>true</mask-password>
<password-codec>org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec;key=hello world</password-codec>
```

When so configured, the Apache ActiveMQ Artemis security manager will initialize a
DefaultSensitiveStringCodec with the parameters "key"-\>"hello world",
then use it to decode all the masked passwords in this configuration
file.

### Choosing a decoder for password masking

As described in the previous sections, all password masking requires a
decoder. A decoder uses an algorithm to convert a masked password into
its original clear text form in order to be used in various security
operations. The algorithm used for decoding must match that for
encoding. Otherwise the decoding may not be successful.

For user's convenience Apache ActiveMQ Artemis provides a default built-in Decoder.
However a user can if they so wish implement their own.

#### The built-in Decoder

Whenever no decoder is specified in the configuration file, the built-in
decoder is used. The class name for the built-in decoder is
org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec. It has both
encoding and decoding capabilities. It uses java.crypto.Cipher utilities
to encrypt (encode) a plaintext password and decrypt a mask string using
same algorithm. Using this decoder/encoder is pretty straightforward. To
get a mask for a password, just run the main class at org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec.

An easy way to do it is through activemq-tools-<VERSION>-jar-with-dependencies.jar since it has all the dependencies:

```sh
    java -cp activemq-tools-6.0.0-jar-with-dependencies.jar org.apache.activemq.artemis.utils.DefaultSensitiveStringCodec "your plaintext password"
```

If you don't want to use the jar-with-dependencies, make sure the classpath is correct. You'll get something like

```
    Encoded password: 80cf731af62c290
```

Just copy "80cf731af62c290" and replace your plaintext password with it.

#### Using a different decoder

It is possible to use a different decoder rather than the built-in one.
Simply make sure the decoder is in Apache ActiveMQ Artemis's classpath and configure
the server to use it as follows:

```xml
    <password-codec>com.foo.SomeDecoder;key1=value1;key2=value2</password-codec>
```

If your decoder needs params passed to it you can do this via key/value
pairs when configuring. For instance if your decoder needs say a
"key-location" parameter, you can define like so:

```xml
    <password-codec>com.foo.NewDecoder;key-location=/some/url/to/keyfile</password-codec>
```

Then configure your cluster-password like this:

```xml
    <mask-password>true</mask-password>
    <cluster-password>masked_password</cluster-password>
```

When Apache ActiveMQ Artemis reads the cluster-password it will initialize the
NewDecoder and use it to decode "mask\_password". It also process all
passwords using the new defined decoder.

#### Implementing your own codecs

To use a different decoder than the built-in one, you either pick one
from existing libraries or you implement it yourself. All decoders must
implement the `org.apache.activemq.utils.SensitiveDataCodec<T>`
interface:

``` java
public interface SensitiveDataCodec<T>
{
   T decode(Object mask) throws Exception;

   void init(Map<String, String> params);
}
```

This is a generic type interface but normally for a password you just
need String type. So a new decoder would be defined like

```java
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
```

Last but not least, once you get your own decoder, please add it to the
classpath. Otherwise Apache ActiveMQ Artemis will fail to load it!
