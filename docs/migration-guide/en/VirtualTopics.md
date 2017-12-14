Virtual Topics
==============

Virtual Topics (a specialisation of virtual destinations) in ActiveMQ 5.x typically address two different but related
problems. Lets take each in turn:
 
Shared access to a JMS durable topic subscription
-------------------------------------------------
With JMS1.1, a durable subscription is identified by the pair of clientId and subscriptionName. The clientId
component must be unique to a connection on the broker. This means that the subscription is exclusive. It is
not possible to load balance the stream of messages across consumers and quick failover is difficult because the
existing connection state on the broker needs to be first disposed.
With virtual topics, each subscription's stream of messages is redirected to a queue.
 
JMS2.0 adds the possibility of shared subscriptions with new API's that are fully supported in Artemis.
Secondly, Artemis uses a queue per topic subscriber model internally and it is possibly to directly address the
subscription queue using it's Fully Qualified Queue name (FQQN).

For example, a default 5.x consumer for topic `VirtualTopic.Orders` subscription `A`:
```
    ...
    Queue subscriptionQueue = session.createQueue("Consumer.A.VirtualTopic.Orders");
    session.createConsumer(subscriptionQueue);

``` 
would be replaced with an Artemis FQQN comprised of the address and queue.
```
    ...
    Queue subscriptionQueue = session.createQueue("VirtualTopic.Orders::Consumer.A");
    session.createConsumer(subscriptionQueue);
```

Durable topic subscribers in a network of brokers
-------------------------------------------------
The store and forward network bridges in 5.x create a durable subscriber per destination. As demand migrates across a
network, duplicate durable subs get created on each node in the network but they do not migrate. The end result can
result in duplicate message storage and ultimately duplicate delivery, which is not good.
When durable subscribers map to virtual topic subscriber queues, the queues can migrate and the problem can be avoided.

In Artemis, because a durable sub is modeled as a queue, this problem does not arise.