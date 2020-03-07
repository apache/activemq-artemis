# Message Group Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to configure and use message groups with ActiveMQ Artemis.

Message groups are sets of messages that has the following characteristics:

*   Messages in a message group share the same group id, i.e. they have same JMSXGroupID string property values.
*   Messages in a message group will be all delivered to no more than one of the queue's consumers. The consumer that receives the first message of a group will receive all the messages that belong to the group.

You can make any message belong to a message group by setting its 'JMSXGroupID' string property to the group id. In this example we create a message group 'Group-0'. And make such a message group of 10 messages. It also create two consumers on the queue where the 10 'Group-0' group messages are to be sent. You can see that with message grouping enabled, all the 10 messages will be received by the first consumer. The second consumer will receive none.

Alternatively, ActiveMQ's connection factories can be configured to _auto group_ messages. By setting `autoGroup=true` in the client's URL a random unique id will be picked to create a message group. _Every messages_ sent by a producer created from this connection factory will automatically be part of this message group.