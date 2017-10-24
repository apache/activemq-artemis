# JMS Durable Subscription Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the broker manually.

This example demonstrates how to use a durable subscription with ActiveMQ Artemis.

Durable subscriptions are a standard part of JMS, please consult the JMS 1.1 specification for full details.

Unlike non durable subscriptions, the key function of durable subscriptions is that the messages contained in them persist longer than the lifetime of the subscriber - i.e. they will accumulate messages sent to the topic even if the subscriber is not currently connected. They will also survive broker restarts. Note that for the messages to be persisted, the messages sent to them must be marked as persistent messages.