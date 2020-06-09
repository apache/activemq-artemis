# Consumer Priority

Consumer priorities allow you to ensure that high priority consumers receive messages while they are active.

Normally, active consumers connected to a queue receive messages from it in a round-robin fashion. When consumer priorities are in use, messages are delivered round-robin if multiple active consumers exist with the same high priority.

Messages will only going to lower priority consumers when the high priority consumers do not have credit available to consume the message, or those high priority consumers have declined to accept the message (for instance because it does not meet the criteria of any selectors associated with the consumer).

Where a consumer does not set, the default priority <b>0</b> is used.

## Core 

#### JMS Example


When using the JMS Client you can set the priority to be used, by using address parameters when 
creating the destination used by the consumer.

```java
Queue queue = session.createQueue("my.destination.name?consumer-priority=50");
Topic topic = session.createTopic("my.destination.name?consumer-priority=50");

consumer = session.createConsumer(queue);
```

The range of priority values is -2<sup>31</sup> to 2<sup>31</sup>-1.

## OpenWire 

####JMS Example

The priority for a consumer is set using Destination Options as follows:

```java
queue = new ActiveMQQueue("TEST.QUEUE?consumer.priority=10");
consumer = session.createConsumer(queue);
```

Because of the limitation of OpenWire, the range of priority values is: 0 to 127. The highest priority is 127.

## AMQP

In AMQP 1.0 the priority of the consumer is set in the properties map of the attach frame where the broker side of the link represents the sending side of the link. 

The key for the entry must be the literal string priority, and the value of the entry must be an integral number in the range -2<sup>31</sup> to 2<sup>31</sup>-1.
