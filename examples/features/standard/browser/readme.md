# JMS QueueBrowser Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example shows you how to use a JMS [QueueBrowser](https://docs.oracle.com/javaee/7/api/javax/jms/QueueBrowser.html) with ActiveMQ Artemis.

Queues are a standard part of JMS, please consult the JMS 1.1 specification for full details.

A QueueBrowser is used to look at messages on the queue without removing them. It can scan the entire content of a queue or only messages matching a message selector.

The example will send 2 messages on a queue, use a QueueBrowser to browse the queue (looking at the message without removing them) and finally consume the 2 messages.