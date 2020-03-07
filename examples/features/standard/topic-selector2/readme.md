# JMS Topic Selector Example 2

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to selectively consume messages using message selectors with topic consumers.

Message selectors are strings with special syntax that can be used in creating consumers. Message consumers that are thus created only receive messages that match its selector. On message delivering, the ActiveMQ Server evaluates the corresponding message headers of the messages against each selector, if any, and then delivers the 'matched' messages to its consumer. Please consult the JMS 1.1 specification for full details.

In this example, three message consumers are created on a topic. The first consumer is created with selector `'color=red'`, it only receives messages that have a 'color' string property of 'red' value; the second is created with selector `'color=green'`, it only receives messages who have a 'color' string property of 'green' value; and the third without a selector, which means it receives all messages. To illustrate, three messages with different 'color' property values are created and sent.