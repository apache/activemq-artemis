# Management

Apache ActiveMQ has an extensive management API that allows a user to modify a
server configuration, create new resources (e.g. JMS queues and topics),
inspect these resources (e.g. how many messages are currently held in a
queue) and interact with it (e.g. to remove messages from a queue). All
the operations allows a client to *manage* Apache ActiveMQ. It also allows
clients to subscribe to management notifications.

There are 3 ways to manage Apache ActiveMQ:

-   Using JMX -- JMX is the standard way to manage Java applications

-   Using the core API -- management operations are sent to Apache ActiveMQ
    server using *core messages*

-   Using the JMS API -- management operations are sent to Apache ActiveMQ
    server using *JMS messages*

Although there are 3 different ways to manage Apache ActiveMQ each API supports
the same functionality. If it is possible to manage a resource using JMX
it is also possible to achieve the same result using Core messages or
JMS messages.

This choice depends on your requirements, your application settings and
your environment to decide which way suits you best.

## The Management API

Regardless of the way you *invoke* management operations, the management
API is the same.

For each *managed resource*, there exists a Java interface describing
what can be invoked for this type of resource.

Apache ActiveMQ exposes its managed resources in 2 packages:

-   *Core* resources are located in the
    `org.apache.activemq.api.core.management` package

-   *JMS* resources are located in the
    `org.apache.activemq.api.jms.management` package

The way to invoke a *management operations* depends whether JMX, core
messages, or JMS messages are used.

> **Note**
>
> A few management operations requires a `filter` parameter to chose
> which messages are involved by the operation. Passing `null` or an
> empty string means that the management operation will be performed on
> *all messages*.

### Core Management API

Apache ActiveMQ defines a core management API to manage core resources. For
full details of the API please consult the javadoc. In summary:

#### Core Server Management

-   Listing, creating, deploying and destroying queues

    A list of deployed core queues can be retrieved using the
    `getQueueNames()` method.

    Core queues can be created or destroyed using the management
    operations `createQueue()` or `deployQueue()` or `destroyQueue()`)on
    the `ActiveMQServerControl` (with the ObjectName
    `org.apache.activemq:module=Core,type=Server` or the resource name
    `core.server`)

    `createQueue` will fail if the queue already exists while
    `deployQueue` will do nothing.

-   Pausing and resuming Queues

    The `QueueControl` can pause and resume the underlying queue. When a
    queue is paused, it will receive messages but will not deliver them.
    When it's resumed, it'll begin delivering the queued messages, if
    any.

-   Listing and closing remote connections

    Client's remote addresses can be retrieved using
    `listRemoteAddresses()`. It is also possible to close the
    connections associated with a remote address using the
    `closeConnectionsForAddress()` method.

    Alternatively, connection IDs can be listed using
    `listConnectionIDs()` and all the sessions for a given connection ID
    can be listed using `listSessions()`.

-   Transaction heuristic operations

    In case of a server crash, when the server restarts, it it possible
    that some transaction requires manual intervention. The
    `listPreparedTransactions()` method lists the transactions which are
    in the prepared states (the transactions are represented as opaque
    Base64 Strings.) To commit or rollback a given prepared transaction,
    the `commitPreparedTransaction()` or `rollbackPreparedTransaction()`
    method can be used to resolve heuristic transactions. Heuristically
    completed transactions can be listed using the
    `listHeuristicCommittedTransactions()` and
    `listHeuristicRolledBackTransactions` methods.

-   Enabling and resetting Message counters

    Message counters can be enabled or disabled using the
    `enableMessageCounters()` or `disableMessageCounters()` method. To
    reset message counters, it is possible to invoke
    `resetAllMessageCounters()` and `resetAllMessageCounterHistories()`
    methods.

-   Retrieving the server configuration and attributes

    The `ActiveMQServerControl` exposes Apache ActiveMQ server configuration
    through all its attributes (e.g. `getVersion()` method to retrieve
    the server's version, etc.)

-   Listing, creating and destroying Core bridges and diverts

    A list of deployed core bridges (resp. diverts) can be retrieved
    using the `getBridgeNames()` (resp. `getDivertNames()`) method.

    Core bridges (resp. diverts) can be created or destroyed using the
    management operations `createBridge()` and `destroyBridge()` (resp.
    `createDivert()` and `destroyDivert()`) on the
    `ActiveMQServerControl` (with the ObjectName
    `org.apache.activemq:module=Core,type=Server` or the resource name
    `core.server`).

-   It is possible to stop the server and force failover to occur with
    any currently attached clients.

    to do this use the `forceFailover()` on the `ActiveMQServerControl`
    (with the ObjectName `org.apache.activemq:module=Core,type=Server`
    or the resource name `core.server`)

    > **Note**
    >
    > Since this method actually stops the server you will probably
    > receive some sort of error depending on which management service
    > you use to call it.

#### Core Address Management

Core addresses can be managed using the `AddressControl` class (with the
ObjectName `org.apache.activemq:module=Core,type=Address,name="<the
                  address name>"` or the resource name
`core.address.<the
                  address name>`).

-   Modifying roles and permissions for an address

    You can add or remove roles associated to a queue using the
    `addRole()` or `removeRole()` methods. You can list all the roles
    associated to the queue with the `getRoles()` method

#### Core Queue Management

The bulk of the core management API deals with core queues. The
`QueueControl` class defines the Core queue management operations (with
the ObjectName
`org.apache.activemq:module=Core,type=Queue,address="<the bound
                  address>",name="<the queue name>"` or the resource
name `core.queue.<the queue name>`).

Most of the management operations on queues take either a single message
ID (e.g. to remove a single message) or a filter (e.g. to expire all
messages with a given property.)

-   Expiring, sending to a dead letter address and moving messages

    Messages can be expired from a queue by using the `expireMessages()`
    method. If an expiry address is defined, messages will be sent to
    it, otherwise they are discarded. The queue's expiry address can be
    set with the `setExpiryAddress()` method.

    Messages can also be sent to a dead letter address with the
    `sendMessagesToDeadLetterAddress()` method. It returns the number of
    messages which are sent to the dead letter address. If a dead letter
    address is not defined, message are removed from the queue and
    discarded. The queue's dead letter address can be set with the
    `setDeadLetterAddress()` method.

    Messages can also be moved from a queue to another queue by using
    the `moveMessages()` method.

-   Listing and removing messages

    Messages can be listed from a queue by using the `listMessages()`
    method which returns an array of `Map`, one `Map` for each message.

    Messages can also be removed from the queue by using the
    `removeMessages()` method which returns a `boolean` for the single
    message ID variant or the number of removed messages for the filter
    variant. The `removeMessages()` method takes a `filter` argument to
    remove only filtered messages. Setting the filter to an empty string
    will in effect remove all messages.

-   Counting messages

    The number of messages in a queue is returned by the
    `getMessageCount()` method. Alternatively, the `countMessages()`
    will return the number of messages in the queue which *match a given
    filter*

-   Changing message priority

    The message priority can be changed by using the
    `changeMessagesPriority()` method which returns a `boolean` for the
    single message ID variant or the number of updated messages for the
    filter variant.

-   Message counters

    Message counters can be listed for a queue with the
    `listMessageCounter()` and `listMessageCounterHistory()` methods
    (see Message Counters section). The message counters can also be
    reset for a single queue using the `resetMessageCounter()` method.

-   Retrieving the queue attributes

    The `QueueControl` exposes Core queue settings through its
    attributes (e.g. `getFilter()` to retrieve the queue's filter if it
    was created with one, `isDurable()` to know whether the queue is
    durable or not, etc.)

-   Pausing and resuming Queues

    The `QueueControl` can pause and resume the underlying queue. When a
    queue is paused, it will receive messages but will not deliver them.
    When it's resume, it'll begin delivering the queued messages, if
    any.

#### Other Core Resources Management

Apache ActiveMQ allows to start and stop its remote resources (acceptors,
diverts, bridges, etc.) so that a server can be taken off line for a
given period of time without stopping it completely (e.g. if other
management operations must be performed such as resolving heuristic
transactions). These resources are:

-   Acceptors

    They can be started or stopped using the `start()` or. `stop()`
    method on the `AcceptorControl` class (with the ObjectName
    `org.apache.activemq:module=Core,type=Acceptor,name="<the acceptor name>"`
    or the resource name
    `core.acceptor.<the address name>`). The acceptors parameters
    can be retrieved using the `AcceptorControl` attributes (see [Understanding Acceptors](configuring-transports.md))

-   Diverts

    They can be started or stopped using the `start()` or `stop()`
    method on the `DivertControl` class (with the ObjectName
    `org.apache.activemq:module=Core,type=Divert,name=<the divert name>`
    or the resource name `core.divert.<the divert name>`). Diverts
    parameters can be retrieved using the `DivertControl` attributes
    (see [Diverting and Splitting Message Flows)](diverts.md))

-   Bridges

    They can be started or stopped using the `start()` (resp. `stop()`)
    method on the `BridgeControl` class (with the ObjectName
    `org.apache.activemq:module=Core,type=Bridge,name="<the bridge name>"`
    or the resource name
    `core.bridge.<the bridge name>`). Bridges parameters can be retrieved
    using the `BridgeControl` attributes (see [Core bridges](core-bridges.md))

-   Broadcast groups

    They can be started or stopped using the `start()` or `stop()`
    method on the `BroadcastGroupControl` class (with the ObjectName
    `org.apache.activemq:module=Core,type=BroadcastGroup,name="<the broadcast group name>"` or the resource name
    `core.broadcastgroup.<the broadcast group name>`). Broadcast groups
    parameters can be retrieved using the `BroadcastGroupControl`
    attributes (see [Clusters](clusters.md))

-   Discovery groups

    They can be started or stopped using the `start()` or `stop()`
    method on the `DiscoveryGroupControl` class (with the ObjectName
    `org.apache.activemq:module=Core,type=DiscoveryGroup,name="<the discovery group name>"` or the resource name
    `core.discovery.<the discovery group name>`). Discovery groups
    parameters can be retrieved using the `DiscoveryGroupControl`
    attributes (see [Clusters](clusters.md))

-   Cluster connections

    They can be started or stopped using the `start()` or `stop()`
    method on the `ClusterConnectionControl` class (with the ObjectName
    `org.apache.activemq:module=Core,type=ClusterConnection,name="<the cluster connection name>"` or the resource name
    `core.clusterconnection.<the cluster connection name>`). Cluster
    connections parameters can be retrieved using the
    `ClusterConnectionControl` attributes (see [Clusters](clusters.md))

### JMS Management API

Apache ActiveMQ defines a JMS Management API to manage JMS *administrated
objects* (i.e. JMS queues, topics and connection factories).

#### JMS Server Management

JMS Resources (connection factories and destinations) can be created
using the `JMSServerControl` class (with the ObjectName
`org.apache.activemq:module=JMS,type=Server` or the resource name
`jms.server`).

-   Listing, creating, destroying connection factories

    Names of the deployed connection factories can be retrieved by the
    `getConnectionFactoryNames()` method.

    JMS connection factories can be created or destroyed using the
    `createConnectionFactory()` methods or `destroyConnectionFactory()`
    methods. These connection factories are bound to JNDI so that JMS
    clients can look them up. If a graphical console is used to create
    the connection factories, the transport parameters are specified in
    the text field input as a comma-separated list of key=value (e.g.
    `key1=10, key2="value", key3=false`). If there are multiple
    transports defined, you need to enclose the key/value pairs between
    curly braces. For example `{key=10}, {key=20}`. In that case, the
    first `key` will be associated to the first transport configuration
    and the second `key` will be associated to the second transport
    configuration (see [Configuring Transports](configuring-transports.md)
    for a list of the transport parameters)

-   Listing, creating, destroying queues

    Names of the deployed JMS queues can be retrieved by the
    `getQueueNames()` method.

    JMS queues can be created or destroyed using the `createQueue()`
    methods or `destroyQueue()` methods. These queues are bound to JNDI
    so that JMS clients can look them up

-   Listing, creating/destroying topics

    Names of the deployed topics can be retrieved by the
    `getTopicNames()` method.

    JMS topics can be created or destroyed using the `createTopic()` or
    `destroyTopic()` methods. These topics are bound to JNDI so that JMS
    clients can look them up

-   Listing and closing remote connections

    JMS Clients remote addresses can be retrieved using
    `listRemoteAddresses()`. It is also possible to close the
    connections associated with a remote address using the
    `closeConnectionsForAddress()` method.

    Alternatively, connection IDs can be listed using
    `listConnectionIDs()` and all the sessions for a given connection ID
    can be listed using `listSessions()`.

#### JMS ConnectionFactory Management

JMS Connection Factories can be managed using the
`ConnectionFactoryControl` class (with the ObjectName
`org.apache.activemq:module=JMS,type=ConnectionFactory,name="<the connection factory
                  name>"` or the resource name
`jms.connectionfactory.<the
                  connection factory name>`).

-   Retrieving connection factory attributes

    The `ConnectionFactoryControl` exposes JMS ConnectionFactory
    configuration through its attributes (e.g. `getConsumerWindowSize()`
    to retrieve the consumer window size for flow control,
    `isBlockOnNonDurableSend()` to know whether the producers created
    from the connection factory will block or not when sending
    non-durable messages, etc.)

#### JMS Queue Management

JMS queues can be managed using the `JMSQueueControl` class (with the
ObjectName `org.apache.activemq:module=JMS,type=Queue,name="<the queue
                  name>"` or the resource name `jms.queue.<the queue
                  name>`).

*The management operations on a JMS queue are very similar to the
operations on a core queue.*

-   Expiring, sending to a dead letter address and moving messages

    Messages can be expired from a queue by using the `expireMessages()`
    method. If an expiry address is defined, messages will be sent to
    it, otherwise they are discarded. The queue's expiry address can be
    set with the `setExpiryAddress()` method.

    Messages can also be sent to a dead letter address with the
    `sendMessagesToDeadLetterAddress()` method. It returns the number of
    messages which are sent to the dead letter address. If a dead letter
    address is not defined, message are removed from the queue and
    discarded. The queue's dead letter address can be set with the
    `setDeadLetterAddress()` method.

    Messages can also be moved from a queue to another queue by using
    the `moveMessages()` method.

-   Listing and removing messages

    Messages can be listed from a queue by using the `listMessages()`
    method which returns an array of `Map`, one `Map` for each message.

    Messages can also be removed from the queue by using the
    `removeMessages()` method which returns a `boolean` for the single
    message ID variant or the number of removed messages for the filter
    variant. The `removeMessages()` method takes a `filter` argument to
    remove only filtered messages. Setting the filter to an empty string
    will in effect remove all messages.

-   Counting messages

    The number of messages in a queue is returned by the
    `getMessageCount()` method. Alternatively, the `countMessages()`
    will return the number of messages in the queue which *match a given
    filter*

-   Changing message priority

    The message priority can be changed by using the
    `changeMessagesPriority()` method which returns a `boolean` for the
    single message ID variant or the number of updated messages for the
    filter variant.

-   Message counters

    Message counters can be listed for a queue with the
    `listMessageCounter()` and `listMessageCounterHistory()` methods
    (see Message Counters section)

-   Retrieving the queue attributes

    The `JMSQueueControl` exposes JMS queue settings through its
    attributes (e.g. `isTemporary()` to know whether the queue is
    temporary or not, `isDurable()` to know whether the queue is durable
    or not, etc.)

-   Pausing and resuming queues

    The `JMSQueueControl` can pause and resume the underlying queue.
    When the queue is paused it will continue to receive messages but
    will not deliver them. When resumed again it will deliver the
    enqueued messages, if any.

#### JMS Topic Management

JMS Topics can be managed using the `TopicControl` class (with the
ObjectName `org.apache.activemq:module=JMS,type=Topic,name="<the topic
                  name>"` or the resource name `jms.topic.<the topic
                  name>`).

-   Listing subscriptions and messages

    JMS topics subscriptions can be listed using the
    `listAllSubscriptions()`, `listDurableSubscriptions()`,
    `listNonDurableSubscriptions()` methods. These methods return arrays
    of `Object` representing the subscriptions information (subscription
    name, client ID, durability, message count, etc.). It is also
    possible to list the JMS messages for a given subscription with the
    `listMessagesForSubscription()` method.

-   Dropping subscriptions

    Durable subscriptions can be dropped from the topic using the
    `dropDurableSubscription()` method.

-   Counting subscriptions messages

    The `countMessagesForSubscription()` method can be used to know the
    number of messages held for a given subscription (with an optional
    message selector to know the number of messages matching the
    selector)

## Using Management Via JMX

Apache ActiveMQ can be managed using
[JMX](http://www.oracle.com/technetwork/java/javase/tech/javamanagement-140525.html).

The management API is exposed by Apache ActiveMQ using MBeans interfaces.
Apache ActiveMQ registers its resources with the domain `org.apache.activemq`.

For example, the `ObjectName` to manage a JMS Queue `exampleQueue` is:

    org.apache.activemq:module=JMS,type=Queue,name="exampleQueue"

and the MBean is:

    org.apache.activemq.api.jms.management.JMSQueueControl

The MBean's `ObjectName` are built using the helper class
`org.apache.activemq.api.core.management.ObjectNameBuilder`. You can
also use `jconsole` to find the `ObjectName` of the MBeans you want to
manage.

Managing Apache ActiveMQ using JMX is identical to management of any Java
Applications using JMX. It can be done by reflection or by creating
proxies of the MBeans.

### Configuring JMX

By default, JMX is enabled to manage Apache ActiveMQ. It can be disabled by
setting `jmx-management-enabled` to `false` in
`activemq-configuration.xml`:

    <!-- false to disable JMX management for Apache ActiveMQ -->
    <jmx-management-enabled>false</jmx-management-enabled>

If JMX is enabled, Apache ActiveMQ can be managed locally using `jconsole`.

> **Note**
>
> Remote connections to JMX are not enabled by default for security
> reasons. Please refer to [Java Management
> guide](http://docs.oracle.com/javase/6/docs/technotes/guides/management/agent.html)
> to configure the server for remote management (system properties must
> be set in `run.sh` or `run.bat` scripts).

By default, Apache ActiveMQ server uses the JMX domain "org.apache.activemq".
To manage several Apache ActiveMQ servers from the *same* MBeanServer, the JMX
domain can be configured for each individual Apache ActiveMQ server by setting
`jmx-domain` in `activemq-configuration.xml`:

    <!-- use a specific JMX domain for ActiveMQ MBeans -->
    <jmx-domain>my.org.apache.activemq</jmx-domain>

#### MBeanServer configuration

When Apache ActiveMQ is run in standalone, it uses the Java Virtual Machine's
`Platform MBeanServer` to register its MBeans. By default [Jolokia](http://www.jolokia.org/)
is also deployed to allow access to the mbean server via rest.

### Example

See the [chapters](examples.md) chapter for an example which shows how to use a remote connection to JMX
and MBean proxies to manage Apache ActiveMQ.

### Exposing JMX using Jolokia

The default Broker configuration ships with the [Jolokia](http://www.jolokia.org)
http agent deployed as a Web Application. Jolokia is a remote
JMX over HTTP bridge that exposed mBeans, for a full guids as
to how to use refer to [Jolokia Documentation](http://www.jolokia.org/documentation.html),
however a simple example to query thebrokers version would
be to use a brower and go to the URL http://localhost:8161/jolokia/read/org.apache.activemq:module=Core,type=Server/Version.

This would give you back something like the following:

    {"timestamp":1422019706,"status":200,"request":{"mbean":"org.apache.activemq:module=Core,type=Server","attribute":"Version","type":"read"},"value":"6.0.0.SNAPSHOT (Active Hornet, 126)"}

## Using Management Via Core API

The core management API in ActiveMQ is called by sending Core messages
to a special address, the *management address*.

*Management messages* are regular Core messages with well-known
properties that the server needs to understand to interact with the
management API:

-   The name of the managed resource

-   The name of the management operation

-   The parameters of the management operation

When such a management message is sent to the management address,
Apache ActiveMQ server will handle it, extract the information, invoke the
operation on the managed resources and send a *management reply* to the
management message's reply-to address (specified by
`ClientMessageImpl.REPLYTO_HEADER_NAME`).

A `ClientConsumer` can be used to consume the management reply and
retrieve the result of the operation (if any) stored in the reply's
body. For portability, results are returned as a [JSON](http://json.org)
String rather than Java Serialization (the
`org.apache.activemq.api.core.management.ManagementHelper` can be used
to convert the JSON string to Java objects).

These steps can be simplified to make it easier to invoke management
operations using Core messages:

1.  Create a `ClientRequestor` to send messages to the management
    address and receive replies

2.  Create a `ClientMessage`

3.  Use the helper class
    `org.apache.activemq.api.core.management.ManagementHelper` to fill
    the message with the management properties

4.  Send the message using the `ClientRequestor`

5.  Use the helper class
    `org.apache.activemq.api.core.management.ManagementHelper` to
    retrieve the operation result from the management reply

For example, to find out the number of messages in the core queue
`exampleQueue`:

``` java
ClientSession session = ...
ClientRequestor requestor = new ClientRequestor(session, "jms.queue.activemq.management");
ClientMessage message = session.createMessage(false);
ManagementHelper.putAttribute(message, "core.queue.exampleQueue", "messageCount");
session.start();
ClientMessage reply = requestor.request(m);
int count = (Integer) ManagementHelper.getResult(reply);
System.out.println("There are " + count + " messages in exampleQueue");
```

Management operation name and parameters must conform to the Java
interfaces defined in the `management` packages.

Names of the resources are built using the helper class
`org.apache.activemq.api.core.management.ResourceNames` and are
straightforward (`core.queue.exampleQueue` for the Core Queue
`exampleQueue`, `jms.topic.exampleTopic` for the JMS Topic
`exampleTopic`, etc.).

### Configuring Core Management

The management address to send management messages is configured in
`activemq-configuration.xml`:

    <management-address>jms.queue.activemq.management</management-address>

By default, the address is `jms.queue.activemq.management` (it is
prepended by "jms.queue" so that JMS clients can also send management
messages).

The management address requires a *special* user permission `manage` to
be able to receive and handle management messages. This is also
configured in activemq-configuration.xml:

    <!-- users with the admin role will be allowed to manage -->
    <!-- Apache ActiveMQ using management messages        -->
    <security-setting match="jms.queue.activemq.management">
       <permission type="manage" roles="admin" />
    </security-setting>

## Using Management Via JMS

Using JMS messages to manage ActiveMQ is very similar to using core API.

An important difference is that JMS requires a JMS queue to send the
messages to (instead of an address for the core API).

The *management queue* is a special queue and needs to be instantiated
directly by the client:

    Queue managementQueue = ActiveMQJMSClient.createQueue("activemq.management");

All the other steps are the same than for the Core API but they use JMS
API instead:

1.  create a `QueueRequestor` to send messages to the management address
    and receive replies

2.  create a `Message`

3.  use the helper class
    `org.apache.activemq.api.jms.management.JMSManagementHelper` to fill
    the message with the management properties

4.  send the message using the `QueueRequestor`

5.  use the helper class
    `org.apache.activemq.api.jms.management.JMSManagementHelper` to
    retrieve the operation result from the management reply

For example, to know the number of messages in the JMS queue
`exampleQueue`:
``` java
Queue managementQueue = ActiveMQJMSClient.createQueue("activemq.management");

QueueSession session = ...
QueueRequestor requestor = new QueueRequestor(session, managementQueue);
connection.start();
Message message = session.createMessage();
JMSManagementHelper.putAttribute(message, "jms.queue.exampleQueue", "messageCount");
Message reply = requestor.request(message);
int count = (Integer)JMSManagementHelper.getResult(reply);
System.out.println("There are " + count + " messages in exampleQueue");
```
### Configuring JMS Management

Whether JMS or the core API is used for management, the configuration
steps are the same (see Configuring Core Management section).

### Example

See the [examples](examples.md) chapter for an example which shows
how to use JMS messages to manage the Apache ActiveMQ server.

## Management Notifications

Apache ActiveMQ emits *notifications* to inform listeners of potentially
interesting events (creation of new resources, security violation,
etc.).

These notifications can be received by 3 different ways:

-   JMX notifications

-   Core messages

-   JMS messages

### JMX Notifications

If JMX is enabled (see Configuring JMX section), JMX notifications can be received by
subscribing to 2 MBeans:

-   `org.apache.activemq:module=Core,type=Server` for notifications on
    *Core* resources

-   `org.apache.activemq:module=JMS,type=Server` for notifications on
    *JMS* resources

### Core Messages Notifications

Apache ActiveMQ defines a special *management notification address*. Core
queues can be bound to this address so that clients will receive
management notifications as Core messages

A Core client which wants to receive management notifications must
create a core queue bound to the management notification address. It can
then receive the notifications from its queue.

Notifications messages are regular core messages with additional
properties corresponding to the notification (its type, when it
occurred, the resources which were concerned, etc.).

Since notifications are regular core messages, it is possible to use
message selectors to filter out notifications and receives only a subset
of all the notifications emitted by the server.

#### Configuring The Core Management Notification Address

The management notification address to receive management notifications
is configured in `activemq-configuration.xml`:

    <management-notification-address>activemq.notifications</management-notification-address>

By default, the address is `activemq.notifications`.

### JMS Messages Notifications

Apache ActiveMQ's notifications can also be received using JMS messages.

It is similar to receiving notifications using Core API but an important
difference is that JMS requires a JMS Destination to receive the
messages (preferably a Topic).

To use a JMS Destination to receive management notifications, you must
change the server's management notification address to start with
`jms.queue` if it is a JMS Queue or `jms.topic` if it is a JMS Topic:

    <!-- notifications will be consumed from "notificationsTopic" JMS Topic -->
    <management-notification-address>jms.topic.notificationsTopic</management-notification-address>

Once the notification topic is created, you can receive messages from it
or set a `MessageListener`:

``` java
Topic notificationsTopic = ActiveMQJMSClient.createTopic("notificationsTopic");

Session session = ...
MessageConsumer notificationConsumer = session.createConsumer(notificationsTopic);
notificationConsumer.setMessageListener(new MessageListener()
{
   public void onMessage(Message notif)
   {
      System.out.println("------------------------");
      System.out.println("Received notification:");
      try
      {
         Enumeration propertyNames = notif.getPropertyNames();
         while (propertyNames.hasMoreElements())
         {
            String propertyName = (String)propertyNames.nextElement();
            System.out.format("  %s: %s\n", propertyName, notif.getObjectProperty(propertyName));
         }
      }
      catch (JMSException e)
      {
      }
      System.out.println("------------------------");
   }
});
```
### Example

See the [examples](examples.md) chapter for an example which shows how to use a JMS `MessageListener` to receive management notifications from ActiveMQ server.

### Notification Types and Headers

Below is a list of all the different kinds of notifications as well as
which headers are on the messages. Every notification has a
`_HQ_NotifType` (value noted in parentheses) and `_HQ_NotifTimestamp`
header. The timestamp is the un-formatted result of a call to
`java.lang.System.currentTimeMillis()`.

-   `BINDING_ADDED` (0)

    `_HQ_Binding_Type`, `_HQ_Address`, `_HQ_ClusterName`,
    `_HQ_RoutingName`, `_HQ_Binding_ID`, `_HQ_Distance`,
    `_HQ_FilterString`

-   `BINDING_REMOVED` (1)

    `_HQ_Address`, `_HQ_ClusterName`, `_HQ_RoutingName`,
    `_HQ_Binding_ID`, `_HQ_Distance`, `_HQ_FilterString`

-   `CONSUMER_CREATED` (2)

    `_HQ_Address`, `_HQ_ClusterName`, `_HQ_RoutingName`, `_HQ_Distance`,
    `_HQ_ConsumerCount`, `_HQ_User`, `_HQ_RemoteAddress`,
    `_HQ_SessionName`, `_HQ_FilterString`

-   `CONSUMER_CLOSED` (3)

    `_HQ_Address`, `_HQ_ClusterName`, `_HQ_RoutingName`, `_HQ_Distance`,
    `_HQ_ConsumerCount`, `_HQ_User`, `_HQ_RemoteAddress`,
    `_HQ_SessionName`, `_HQ_FilterString`

-   `SECURITY_AUTHENTICATION_VIOLATION` (6)

    `_HQ_User`

-   `SECURITY_PERMISSION_VIOLATION` (7)

    `_HQ_Address`, `_HQ_CheckType`, `_HQ_User`

-   `DISCOVERY_GROUP_STARTED` (8)

    `name`

-   `DISCOVERY_GROUP_STOPPED` (9)

    `name`

-   `BROADCAST_GROUP_STARTED` (10)

    `name`

-   `BROADCAST_GROUP_STOPPED` (11)

    `name`

-   `BRIDGE_STARTED` (12)

    `name`

-   `BRIDGE_STOPPED` (13)

    `name`

-   `CLUSTER_CONNECTION_STARTED` (14)

    `name`

-   `CLUSTER_CONNECTION_STOPPED` (15)

    `name`

-   `ACCEPTOR_STARTED` (16)

    `factory`, `id`

-   `ACCEPTOR_STOPPED` (17)

    `factory`, `id`

-   `PROPOSAL` (18)

    `_JBM_ProposalGroupId`, `_JBM_ProposalValue`, `_HQ_Binding_Type`,
    `_HQ_Address`, `_HQ_Distance`

-   `PROPOSAL_RESPONSE` (19)

    `_JBM_ProposalGroupId`, `_JBM_ProposalValue`,
    `_JBM_ProposalAltValue`, `_HQ_Binding_Type`, `_HQ_Address`,
    `_HQ_Distance`

-   `CONSUMER_SLOW` (21)

    `_HQ_Address`, `_HQ_ConsumerCount`, `_HQ_RemoteAddress`,
    `_HQ_ConnectionName`, `_HQ_ConsumerName`, `_HQ_SessionName`

## Message Counters

Message counters can be used to obtain information on queues *over time*
as Apache ActiveMQ keeps a history on queue metrics.

They can be used to show *trends* on queues. For example, using the
management API, it would be possible to query the number of messages in
a queue at regular interval. However, this would not be enough to know
if the queue is used: the number of messages can remain constant because
nobody is sending or receiving messages from the queue or because there
are as many messages sent to the queue than messages consumed from it.
The number of messages in the queue remains the same in both cases but
its use is widely different.

Message counters gives additional information about the queues:

-   `count`

    The *total* number of messages added to the queue since the server
    was started

-   `countDelta`

    the number of messages added to the queue *since the last message
    counter update*

-   `messageCount`

    The *current* number of messages in the queue

-   `messageCountDelta`

    The *overall* number of messages added/removed from the queue *since
    the last message counter update*. For example, if
    `messageCountDelta` is equal to `-10` this means that overall 10
    messages have been removed from the queue (e.g. 2 messages were
    added and 12 were removed)

-   `lastAddTimestamp`

    The timestamp of the last time a message was added to the queue

-   `udpateTimestamp`

    The timestamp of the last message counter update

These attributes can be used to determine other meaningful data as well.
For example, to know specifically how many messages were *consumed* from
the queue since the last update simply subtract the `messageCountDelta`
from `countDelta`.

### Configuring Message Counters

By default, message counters are disabled as it might have a small
negative effect on memory.

To enable message counters, you can set it to `true` in
`activemq-configuration.xml`:

    <message-counter-enabled>true</message-counter-enabled>

Message counters keeps a history of the queue metrics (10 days by
default) and samples all the queues at regular interval (10 seconds by
default). If message counters are enabled, these values should be
configured to suit your messaging use case in
`activemq-configuration.xml`:

    <!-- keep history for a week -->
    <message-counter-max-day-history>7</message-counter-max-day-history>
    <!-- sample the queues every minute (60000ms) -->
    <message-counter-sample-period>60000</message-counter-sample-period>

Message counters can be retrieved using the Management API. For example,
to retrieve message counters on a JMS Queue using JMX:

``` java
// retrieve a connection to Apache ActiveMQ's MBeanServer
MBeanServerConnection mbsc = ...
JMSQueueControlMBean queueControl = (JMSQueueControl)MBeanServerInvocationHandler.newProxyInstance(mbsc,
   on,
   JMSQueueControl.class,
   false);
// message counters are retrieved as a JSON String
String counters = queueControl.listMessageCounter();
// use the MessageCounterInfo helper class to manipulate message counters more easily
MessageCounterInfo messageCounter = MessageCounterInfo.fromJSON(counters);
System.out.format("%s message(s) in the queue (since last sample: %s)\n",
messageCounter.getMessageCount(),
messageCounter.getMessageCountDelta());
```

### Example

See the [examples](examples.md) chapter for an example which shows how to use message counters to retrieve information on a JMS `Queue`.
