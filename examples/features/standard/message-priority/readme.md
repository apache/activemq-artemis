# JMS Message Priority Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows how messages with different priorities are delivered in different orders.

The Message Priority property carries the delivery preference of sent messages. It can be set by the message's standard header field 'JMSPriority' as defined in JMS specification version 1.1\. The value is of type integer, ranging from 0 (the lowest) to 9 (the highest). When messages are being delivered, their priorities will effect their order of delivery. Messages of higher priorities will likely be delivered before those of lower priorities. Messages of equal priorities are delivered in the natural order of their arrival at their destinations. Please consult the JMS 1.1 specification for full details.

In this example, three messages are sent to a queue with different priorities. The first message is sent with default priority (4), the second is sent with a higher priority (5), and the third has the highest priority (9). At the receiving end, we will show the order of receiving of the three messages. You will see that the third message, though last sent, will 'jump' forward to be the first one received. The second is also received ahead of the message first sent, but behind the third message. The first message, regardless of its being sent first, arrives last.