# Static Message Selector Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to configure a ActiveMQ Artemis queue with static message selectors (filters).

Static message selectors are ActiveMQ's extension to message selectors as defined in JMS spec 1.1. Rather than specifying the selector in the application code, static message selectors are defined in one of ActiveMQ's configuration files, broker.xml, as an element called `filter` inside each queue definition, like

Once configured the queue `selectorQueue` only delivers messages that are selected against the filter, i.e., only the messages whose `color` properties are of `red` values can be received by its consumers. Those that don't match the filter will be dropped by the queue and therefore will never be delivered to any of its consumers.

In the example code, five messages with different `color` property values are sent to queue `selectorQueue`. One consumer is created to receive messages from the queue. Of the five sent messages, two are of `red` color properties, one is `blue`, one is `green` and one has not the `color` property at all. The result is that the consumer only gets the two `red` messages.