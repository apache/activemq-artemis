# Message Grouping

Message groups are sets of messages that have the following characteristics:

- Messages in a message group share the same group id, i.e. they have same
  group identifier property (`JMSXGroupID` for JMS, `_AMQ_GROUP_ID` for Apache
  ActiveMQ Artemis Core API).

- Messages in a message group are always consumed by the same consumer, even if
  there are many consumers on a queue. They pin all messages with the same
  group id to the same consumer. If that consumer closes another consumer is
  chosen and will receive all messages with the same group id.

Message groups are useful when you want all messages for a certain value of the
property to be processed serially by the same consumer.

An example might be orders for a certain stock. You may want orders for any
particular stock to be processed serially by the same consumer. To do this you
can create a pool of consumers (perhaps one for each stock, but less will work
too), then set the stock name as the value of the _AMQ_GROUP_ID property.

This will ensure that all messages for a particular stock will always be
processed by the same consumer.

> **Note:**
>
> Grouped messages can impact the concurrent processing of non-grouped messages
> due to the underlying FIFO semantics of a queue. For example, if there is a
> chunk of 100 grouped messages at the head of a queue followed by 1,000
> non-grouped messages then all the grouped messages will need to be sent to
> the appropriate client (which is consuming those grouped messages serially)
> before any of the non-grouped messages can be consumed. The functional impact
> in this scenario is a temporary suspension of concurrent message processing
> while all the grouped messages are processed. This can be a performance
> bottleneck so keep it in mind when determining the size of your message
> groups, and consider whether or not you should isolate your grouped messages
> from your non-grouped messages.

## Using Core API

The property name used to identify the message group is `"_AMQ_GROUP_ID"` (or
the constant `MessageImpl.HDR_GROUP_ID`). Alternatively, you can set
`autogroup` to true on the `SessionFactory` which will pick a random unique id.

## Using JMS

The property name used to identify the message group is `JMSXGroupID`.

```java
// send 2 messages in the same group to ensure the same
// consumer will receive both
Message message = ...
message.setStringProperty("JMSXGroupID", "Group-0");
producer.send(message);

message = ...
message.setStringProperty("JMSXGroupID", "Group-0");
producer.send(message);
```

Alternatively, you can set `autogroup` to true on the
`ActiveMQConnectonFactory` which will pick a random unique id. This can also be
set in the JNDI context environment, e.g. `jndi.properties`.  Here's a simple
example using the "ConnectionFactory" connection factory which is available in
the context by default

```properties
java.naming.factory.initial=org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory
connectionFactory.myConnectionFactory=tcp://localhost:61616?autoGroup=true
```

Alternatively you can set the group id via the connection factory. All messages
sent with producers created via this connection factory will set the
`JMSXGroupID` to the specified value on all messages sent. This can also be set
in the JNDI context environment, e.g. `jndi.properties`.  Here's a simple
example using the "ConnectionFactory" connection factory which is available in
the context by default:

```properties
java.naming.factory.initial=org.apache.activemq.artemis.jndi.ActiveMQInitialContextFactory
connectionFactory.myConnectionFactory=tcp://localhost:61616?groupID=Group-0
```


#### Closing a Message Group
You generally don't need to close a message group, you just keep using it. 

However if you really do want to close a group you can add a negative sequence number.

Example:
```java
Mesasge message = session.createTextMessage("<foo>hey</foo>");
message.setStringProperty("JMSXGroupID", "Group-0");
message.setIntProperty("JMSXGroupSeq", -1);
...
producer.send(message);
```

This then closes the message group so if another message is sent in the future with the same message group ID it will be reassigned to a new consumer.

#### Notifying Consumer of Group Ownership change

ActiveMQ supports putting a boolean header, set on the first message sent to a consumer for a particular message group.

To enable this, you must set a header key that the broker will use to set the flag.

In the examples we use `JMSXGroupFirstForConsumer` but it can be any header key value you want.


By setting `group-first-key` to `JMSXGroupFirstForConsumer` at the queue level, every time a new group is assigned a consumer the header `JMSXGroupFirstForConsumer` will be set to true on the first message.

```xml
<address name="foo.bar">
   <multicast>
      <queue name="orders1" group-first-key="JMSXGroupFirstForConsumer"/>
   </multicast>
</address>
```

Or on auto-create when using the JMS Client by using address parameters when 
creating the destination used by the consumer.

```java
Queue queue = session.createQueue("my.destination.name?group-first-key=JMSXGroupFirstForConsumer");
Topic topic = session.createTopic("my.destination.name?group-first-key=JMSXGroupFirstForConsumer");
```

Also the default for all queues under and address can be defaulted using the 
`address-setting` configuration:

```xml
<address-setting match="my.address">
   <default-group-first-key>JMSXGroupFirstForConsumer</default-group-first-key>
</address-setting>
```

By default this is null, and therefor OFF. 

#### Rebalancing Message Groups

Sometimes after new consumers are added you can find that if you have long lived groups, that they have no groups assigned, and thus are not being utilised, this is because the long lived groups will already be assigned to existing consumers.

It is possibly to rebalance the groups.

***note*** during the split moment of reset, a message to the original associated consumer could be in flight at the same time, a new message for the same group is dispatched to the new associated consumer.

##### Manually 

via the management API or managment console by invoking `resetAllGroups`

##### Automatically

By setting `group-rebalance` to `true` at the queue level, every time a consumer is added it will trigger a rebalance/reset of the groups.

As noted above, when group rebalance is done, there is a risk you may have inflight messages being processed, by default the broker will continue to dispatch whilst rebalance is occuring. To ensure that inflight messages are processed before dispatch of new messages post rebalance, 
to different consumers, you can set `group-rebalance-pause-dispatch` to `true` which will cause the dispatch to pause whilst rebalance occurs, until all inflight messages are processed.   

```xml
<address name="foo.bar">
   <multicast>
      <queue name="orders1" group-rebalance="true" group-rebalance-pause-dispatch="true"/>
   </multicast>
</address>
```

Or on auto-create when using the JMS Client by using address parameters when 
creating the destination used by the consumer.

```java
Queue queue = session.createQueue("my.destination.name?group-rebalance=true&group-rebalance-pause-dispatch=true");
Topic topic = session.createTopic("my.destination.name?group-rebalance=true&group-rebalance-pause-dispatch=true");
```

Also the default for all queues under and address can be defaulted using the 
`address-setting` configuration:

```xml
<address-setting match="my.address">
   <default-group-rebalance>true</default-group-rebalance>
   <default-group-rebalance-pause-dispatch>true</default-group-rebalance-pause-dispatch>
</address-setting>
```


By default, `default-group-rebalance` is `false` meaning this is disabled/off.
By default, `default-group-rebalance-pause-dispatch` is `false` meaning this is disabled/off.


#### Group Buckets

For handling groups in a queue with bounded memory allowing better scaling of groups, 
you can enable group buckets, essentially the group id is hashed into a bucket instead of keeping track of every single group id.

Setting `group-buckets` to `-1` keeps default behaviour which means the queue keeps track of every group but suffers from unbounded memory use.

Setting `group-buckets` to `0` disables grouping (0 buckets), on a queue. This can be useful on a multicast address, 
where many queues exist but one queue you may not care for ordering and prefer to keep round robin behaviour.

There is a number of ways to set `group-buckets`.


```xml
<address name="foo.bar">
   <multicast>
      <queue name="orders1" group-buckets="1024"/>
   </multicast>
</address>
```

Specified on creating a Queue by using the CORE api specifying the parameter 
`group-buckets` to `20`. 

Or on auto-create when using the JMS Client by using address parameters when 
creating the destination used by the consumer.

```java
Queue queue = session.createQueue("my.destination.name?group-buckets=1024");
Topic topic = session.createTopic("my.destination.name?group-buckets=1024");
```

Also the default for all queues under and address can be defaulted using the 
`address-setting` configuration:

```xml
<address-setting match="my.bucket.address">
   <default-group-buckets>1024</default-group-buckets>
</address-setting>
```

By default, `default-group-buckets` is `-1` this is to keep compatibility with existing default behaviour. 

Address [wildcards](wildcard-syntax.md) can be used to configure group-buckets for a 
set of addresses.

## Example

See the [Message Group Example](examples.md#message-group) which shows how
message groups are configured and used with JMS and via a connection factory.

## Clustered Grouping

Using message groups in a cluster is a bit more complex. This is because
messages with a particular group id can arrive on any node so each node needs
to know about which group id's are bound to which consumer on which node. The
consumer handling messages for a particular group id may be on a different node
of the cluster, so each node needs to know this information so it can route the
message correctly to the node which has that consumer.

To solve this there is the notion of a grouping handler. Each node will have
its own grouping handler and when a messages is sent with a group id assigned,
the handlers will decide between them which route the message should take.

Here is a sample config for each type of handler. This should be configured in
`broker.xml`.

```xml
<grouping-handler name="my-grouping-handler">
   <type>LOCAL</type>
   <address>jms</address>
   <timeout>5000</timeout>
</grouping-handler>

<grouping-handler name="my-grouping-handler">
   <type>REMOTE</type>
   <address>jms</address>
   <timeout>5000</timeout>
</grouping-handler>
```
    
- `type` two types of handlers are supported - `LOCAL` and `REMOTE`.  Each
  cluster should choose 1 node to have a `LOCAL` grouping handler and all the
  other nodes should have `REMOTE` handlers. It's the `LOCAL` handler that
  actually makes the decision as to what route should be used, all the other
  `REMOTE` handlers converse with this. 

- `address` refers to a [cluster connection and the address it
  uses](clusters.md#configuring-cluster-connections). Refer to the clustering
  section on how to configure clusters.
    
- `timeout` how long to wait for a decision to be made. An exception will be
  thrown during the send if this timeout is reached, this ensures that strict
  ordering is kept.

The decision as to where a message should be routed to is initially proposed by
the node that receives the message. The node will pick a suitable route as per
the normal clustered routing conditions, i.e.  round robin available queues,
use a local queue first and choose a queue that has a consumer. If the proposal
is accepted by the grouping handlers the node will route messages to this queue
from that point on, if rejected an alternative route will be offered and the
node will again route to that queue indefinitely. All other nodes will also
route to the queue chosen at proposal time. Once the message arrives at the
queue then normal single server message group semantics take over and the
message is pinned to a consumer on that queue.

You may have noticed that there is a single point of failure with the single
local handler. If this node crashes then no decisions will be able to be made.
Any messages sent will be not be delivered and an exception thrown. To avoid
this happening Local Handlers can be replicated on another backup node. Simple
create your back up node and configure it with the same Local handler.

## Clustered Grouping Best Practices

Some best practices should be followed when using clustered grouping:

1. Make sure your consumers are distributed evenly across the different nodes
   if possible. This is only an issue if you are creating and closing
   consumers regularly. Since messages are always routed to the same queue once
   pinned, removing a consumer from this queue may leave it with no consumers
   meaning the queue will just keep receiving the messages. Avoid closing
   consumers or make sure that you always have plenty of consumers, i.e., if you
   have 3 nodes have 3 consumers.

2. Use durable queues if possible. If queues are removed once a group is bound
   to it, then it is possible that other nodes may still try to route messages
   to it. This can be avoided by making sure that the queue is deleted by the
   session that is sending the messages. This means that when the next message is
   sent it is sent to the node where the queue was deleted meaning a new proposal
   can successfully take place. Alternatively you could just start using a
   different group id.

3. Always make sure that the node that has the Local Grouping Handler is
   replicated. These means that on failover grouping will still occur.

4. In case you are using group-timeouts, the remote node should have a smaller
   group-timeout with at least half of the value on the main coordinator. This
   is because this will determine how often the last-time-use value should be
   updated with a round trip for a request to the group between the nodes.

## Clustered Grouping Example

See the [Clustered Grouping Example](examples.md#clustered-grouping) which
shows how to configure message groups with a ActiveMQ Artemis Cluster.
