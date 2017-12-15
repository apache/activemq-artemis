# Message Group Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to configure and use message groups via a connection factory with ActiveMQ Artemis.

Message groups are sets of messages that has the following characteristics:

*   Messages in a message group share the same group id, i.e. they have same JMSXGroupID string property values.
*   Messages in a message group will be all delivered to no more than one of the queue's consumers. The consumer that receives the first message of a group will receive all the messages that belongs to the group.

You can make any message belong to a message group by setting a 'group-id' on the connection factory. All producers created via this connection factory will set that group id on its messages. In this example we set the group id 'Group-0'on a connection factory and send messages via 2 different producers and check that only 1 consumer receives them.

Alternatively, ActiveMQ's connection factories can be configured to _auto group_ messages. By setting `autoGroup=true` in the client's URL a random unique id will be picked to create a message group. _Every messages_ sent by a producer created from this connection factory will automatically be part of this message group.