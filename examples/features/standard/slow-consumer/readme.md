# JMS Slow Consumer Example

To run the example, simply type **mvn verify** from this directory, or **mvn -PnoServer verify** if you want to start and create the server manually.

This example demonstrates Slow Consumer policy KILL and NOTIFY and the associated address-settings.

How often the broker checks for Slow Consumers is configurable by **slow-consumer-check-period** in the brokers address-settings. The default value is for **slow-consumer-check-period** is 5 seconds. A broker considers a consumer slow if the **slow-consumer-threshold** is not been met. The **slow-consumer-threshold** is the number of messages consumed by the consumer within a second. When a slow consumer is detected, the broker action depends on which **slow-consumer-policy** is configured.

The **slow-consumer-policy** **KILL** will kill the consumers connection

The **slow-consumer-policy** **NOTIFY** will send a CONSUMER_SLOW management notification that an application can receive

There are 2 example clients:

**KillSlowConsumerExample** sends messages to a queue "slow.consumer.kill". It then starts a consumer BUT does not consume any messages. It waits for 8 seconds and tries to consume a message. It expects to receive an exception as the connection should already be closed.

**NotifySlowConsumerExample** sends messages to a queue "slow.consumer.notify". It creates a consumer on the topic "notify.topic" that has been configured as the broker's **management-notification-address**. It then starts a consumer on "slow.consumer.notify" BUT does not consume any messages. The consumer on "notify.topic" will receive a CONSUMER_SLOW management notification and then stop the client.