# JMS Queue Selector Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to selectively consume messages using message selectors with queue consumers.

Message selectors are strings with special syntax that can be used in creating consumers. Message consumers created with a message selector will only receive messages that match its selector. On message delivery, the JBoss Message Server evaluates the corresponding message headers of the messages against each selector, if any, and then delivers the 'matched' messages to its consumer. Please consult the JMS 1.1 specification for full details.

In this example, three message consumers are created on a queue. The first consumer is created with selector `'color=red'`, it only receives messages that have a 'color' string property of 'red' value; the second is created with selector `'color=green'`, it only receives messages who have a 'color' string property of 'green' value; and the third without a selector, which means it receives all messages. To illustrate, three messages with different 'color' property values are created and sent.

Selectors can be used with both queue consumers and topic consumers. The difference is that with queue consumers, a message is only delivered to one consumer on the queue, while topic consumers the message will be delivered to every matching consumers. In this example, if the third consumer (anyConsumer) were the first consumer created, it will consume the first message delivered, therefore there is no chance for the next consumer to get the message, even if it matches the selector.